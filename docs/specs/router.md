# Rust vLLM event-driven KV-aware Router 设计

## 1. 背景与目标

本项目实现一个面向 vLLM 的、事件驱动的 KV-aware 路由数据面。项目使用 Rust
编写，只解决单模型文本推理中的请求选路问题，不建设完整推理平台，也不实现集群
控制面。

本设计只有一次完整交付，不按阶段拆分。交付目标是：

- 使用现成 Inference Gateway 暴露 OpenAI-compatible API。
- mini-router 作为 Gateway 的内部 Endpoint Picker，不暴露客户端推理 API。
- Router 遵循 Gateway API Inference Extension 的 Endpoint Picker Protocol。
- Router 对 Kubernetes 兼容，但不调用或监听 Kubernetes API。
- Gateway 在每次选路请求中提供当前允许选择的 candidate endpoint subset。
- Router 在选路前只执行一次 chat rendering 和 tokenization。
- 同一份 `token_ids` 同时用于 KV prefix 匹配和最终 vLLM 推理。
- Router 消费 vLLM 发布的真实 KV cache events，在内存中维护 endpoint cache view。
- Router 综合连续 prefix 命中和实时负载选择 endpoint。
- 每个 Router 副本独立维护本地状态，不实现跨副本同步。
- 默认跨组件测试运行在无 GPU 的 kind 集群中，通过真实 Gateway、真实 Router/adapter
  和可控的 vLLM 测试替身验证完整链路。

本项目不传输、复制或持久化真实 KV Tensor。

## 2. 明确范围

### 2.1 支持范围

- 单个固定模型。
- 多个运行同一模型和相同 revision 的 vLLM Worker。
- `/v1/chat/completions`。
- text-only `messages`。
- 普通非流式和 streaming 响应。
- vLLM prefix caching 和 KV cache events。
- 一种正式路由策略：KV prefix overlap 与 Worker load 的混合评分。
- KV 状态不可用时的 Least Load 降级。
- Envoy AI Gateway 或其他兼容 Gateway API Inference Extension 的 Gateway。
- 由 EPP request 提供 candidate endpoint subset。
- 使用 kind 运行默认的无 GPU Kubernetes 集成测试。

### 2.2 不支持范围

- Kubernetes client、Informer、Controller 或 Operator。
- 自定义 CRD、EndpointSlice 管理和集群 Leader Election。
- Worker 自注册、Lease、分布式成员管理和 Router 状态复制。
- 多模型和动态模型加载。
- 多模态输入。
- LoRA adapter 路由。
- tool calling、reasoning parser 和 structured output 的专项兼容。
- `/v1/completions`、Responses API 和其他 OpenAI endpoint。
- Prefill/Decode disaggregation。
- KV Tensor 跨 Worker 传输或复制。
- CPU、SSD、远端 KV cache placement。
- autoscaling、请求迁移和 stream resume。
- 语义路由。
- 自研 Gateway、鉴权、限流或公网 API server。

不在支持范围内的请求由 Gateway 或 Router 拒绝，不静默采用不精确的 KV-aware 行为。

## 3. 核心架构决策

### 3.1 对 Kubernetes 兼容，但不感知 Kubernetes

mini-router 不依赖 Kubernetes API。生产部署中，Kubernetes、InferencePool 和 Gateway
负责 Worker membership、readiness、Pod 生命周期和扩缩容；Gateway 把本次请求允许
选择的 endpoint subset 传给 Router。

Router 将每次请求中的 candidate subset 视为本次决策的权威边界：

- 只能选择 subset 中的 endpoint。
- 本地仍有状态、但不在当前 subset 中的 endpoint 不能被选择。
- 第一次看到的新 endpoint 会触发 metrics 和 KV event consumer 的懒加载。
- 长时间不再出现的 endpoint 由本地 TTL 清理。

因此 Router 不需要实现集群成员一致性。多个 Router 副本可以由 Kubernetes 普通
Deployment 托管，各自独立消费事件并最终收敛；短暂状态差异只影响缓存命中收益，
不影响推理正确性。

### 3.2 客户端入口使用现成 Gateway

Gateway 是外部依赖，不属于 mini-router 的实现范围：

| 环境 | Gateway | candidate 来源 |
|---|---|---|
| 生产 Kubernetes | Envoy AI Gateway 或兼容实现 | EPP request metadata |
| kind 集成测试 | 与生产相同的 Gateway 实现 | EPP request metadata |

Gateway 负责：

- 暴露 `/v1/chat/completions`。
- 鉴权、限流、请求大小限制和 TLS。
- 缓冲用于选路的 request body。
- 提供 `x-gateway-destination-endpoint-subset`。
- 调用 mini-router 获得目标 endpoint 和 body mutation。
- 将修改后的请求发送到目标 Worker 的 token-in adapter。
- 向客户端透传 vLLM 的非流式或 streaming response。

Router 不持有客户端 streaming connection，也不代理生成响应。为符合 Endpoint Picker
Protocol，ext-proc control stream 可以在响应阶段接收
`x-gateway-destination-endpoint-served` metadata，但 response body processing 必须关闭。

### 3.3 mini-router 使用 Rust

mini-router 是一个异步 Rust 服务，基于 Tokio 运行。它包含：

- Envoy ext-proc / EPP 协议适配。
- candidate subset 解析与本地 endpoint state 管理。
- vLLM Renderer client。
- vLLM KV event consumer。
- Worker load collector。
- Prefix Index。
- Routing Scorer。

Rust 代码不重写 Hugging Face tokenizer、chat template 或 vLLM renderer。

### 3.4 Render Once，不重复 tokenization

每个 Router Pod 旁部署一个 CPU-only vLLM Renderer sidecar：

```bash
vllm launch render <model> \
  --host 127.0.0.1 \
  --port 8100
```

Renderer 加载模型配置、tokenizer 和 chat template，但不加载模型权重、不使用 GPU。
它把 OpenAI chat request 转换为 Render API 响应中的 `token_ids`。Router 内部、
routing envelope 和 KV 查询统一使用同一个字段名 `token_ids`。

Router 使用这些 token IDs 查询 Prefix Index，然后通过 ext-proc body mutation 将同一份
token IDs 注入发往选中 Worker 的内部请求。Worker 不再执行 chat template 和
tokenization。

```text
OpenAI request
  -> vLLM Renderer
  -> token_ids
  -> KV-aware routing
  -> Rust token-in adapter
  -> vLLM Engine
```

### 3.5 每个 Worker 使用 Rust token-in adapter

每个 vLLM Worker 前部署一个同 Pod 的 Rust token-in adapter。InferencePool 中暴露
的 endpoint 是 adapter endpoint，而不是 vLLM endpoint。

adapter 只负责：

1. 接收内部 chat completion 请求。
2. 请求包含 routing envelope 时，校验 envelope、模型 ID 和 token 数限制。
3. 请求不包含 routing envelope 时，进入受信 Gateway 专用的 raw fallback。
4. 移除内部 routing envelope 和客户端可能伪造的 token-in 参数。
5. 将 Router 的 `token_ids` 转换为固定 vLLM 版本支持的 token-in 参数。
6. 把请求转发到 loopback vLLM `/v1/chat/completions`。
7. 按字节流透传 vLLM response，不解析或重组 SSE。

当前 vLLM 使用：

```json
{
  "kv_transfer_params": {
    "prompt_token_ids": [1, 2, 3]
  }
}
```

该字段目前属于 vLLM 实验接口，必须由 adapter 封装。Gateway、Router Core 和内部
routing envelope 都不直接依赖该字段名。

## 4. 总体架构

```text
Kubernetes Control Plane
  ├── Worker Pod lifecycle/readiness
  ├── InferencePool membership
  └── Router Deployment replicas
                 │
                 ▼
Client ──► Existing Inference Gateway
                 │
                 │ ext-proc/EPP
                 │ body + candidate endpoint subset
                 ▼
           Rust mini-router
           - Renderer Client ──► local vLLM Renderer sidecar
           - Endpoint States
           - KV Event Indexer
           - Prefix Index
           - Load View
           - Routing Scorer
                 │
                 │ selected endpoint + body mutation
                 ▼
           Existing Inference Gateway
                 │
                 ▼
       Selected Rust token-in adapter
                 │ loopback
                 ▼
           Selected vLLM Worker
                 │
                 ├── KV events ──► mini-router replicas
                 ├── metrics ────► mini-router replicas
                 └── response ───► adapter ──► Gateway ──► Client
```

只有 Gateway、Router 和 adapter 之间的标准网络协议进入项目。Kubernetes 资源状态
不会进入 Router Core。kind 测试也遵守相同边界，不向 Router 注入测试专用的成员管理
接口。

## 5. Candidate Endpoint 与本地状态

### 5.1 Candidate subset 是请求级权威边界

Kubernetes 部署要求 Gateway 在 ext-proc metadata 中提供：

```text
x-gateway-destination-endpoint-subset
```

Router 按 Gateway API Inference Extension 约定解析该字段。若字段缺失、为空或所有
endpoint 均无资格参与路由，Router 返回明确的不可选路结果，由 Gateway 执行其配置的
fail-open 或 503 行为。

Router 返回的目标必须同时写入：

- `x-gateway-destination-endpoint` response header。
- Endpoint Picker Protocol 要求的 dynamic metadata。

两处值必须一致。

### 5.2 固定的 Worker Pod 端口约定

candidate endpoint 指向同 Pod 的 Rust token-in adapter：

```text
adapter    <pod-ip>:8200
vLLM HTTP  <pod-ip>:8000
metrics    <pod-ip>:8000/metrics
KV events  tcp://<pod-ip>:5557
KV replay  tcp://<pod-ip>:5558
```

Router 从 candidate endpoint 的 host 和固定配置端口推导 metrics、event 和 replay
地址，不查询 Pod、Service 或 EndpointSlice metadata。

内部键使用 Gateway 给出的规范化 endpoint：

```text
WorkerKey = canonical(candidate endpoint)
```

### 5.3 懒加载 endpoint state

Router 每次收到 candidate subset 时执行：

```text
observe(candidate endpoints)
  ├── 已存在：更新 last_seen
  └── 新 endpoint：
        ├── 创建本地 generation
        ├── 启动 metrics collector
        ├── 连接 KV event subscriber
        └── 尝试 KV replay
```

新 endpoint 的后台状态尚未就绪时仍可参与 fallback，但不能获得 precise KV bonus。
所有 candidates 都没有有效负载数据时，使用 `request_id + endpoint` 的稳定 hash 打破
平局，避免所有首批请求集中到第一个 endpoint。

### 5.4 状态失效与清理

当前请求不包含某 endpoint 时，Router 即使保留其本地索引也不能选择它。

后台 janitor 定期删除超过 `endpoint_state_ttl` 未出现在任何 candidate subset 中的
状态：

1. 停止 metrics collector。
2. 关闭 KV event/replay connection。
3. 清除该 endpoint 的全部 cache ownership。
4. 删除本地 load 和 event watermark。

event connection 重建、sequence 回退或无法修复的 gap 会增加本地 generation，并在
接受新事件前清除旧 ownership。该 generation 只用于单个 Router 进程的状态隔离，
不是分布式身份。

## 6. 请求与内部协议

### 6.1 Gateway 到 Router

Gateway 以 buffered ext-proc request body 调用 Router：

```text
RoutingRequest
  request_id
  model
  openai_chat_body
  candidate_endpoints[]
  received_at
```

Router 验证：

- `model` 等于部署时配置的唯一模型。
- `messages` 存在且所有 content 都是文本。
- request body 和预计 token 数没有超过限制。
- candidate endpoint 格式有效且数量没有超过限制。

### 6.2 Router 到 Renderer

Router 将清除客户端保留字段后的 chat completion body 发送给本机 Renderer：

```text
POST /v1/chat/completions/render
```

Router 只读取：

```text
RenderedInput
  token_ids[]
```

Renderer、adapter 和所有 Worker 必须固定到同一受支持的 vLLM 版本；Renderer 与
Worker 还必须使用完全一致的：

- model revision
- tokenizer revision
- chat template digest
- special token configuration

这些值来自同一个不可变部署配置。Renderer identity 不匹配时 Router 不进入 ready。

### 6.3 Router 返回 endpoint 和 body mutation

```text
RoutingDecision
  decision_id
  selected_endpoint
  local_generation
  matched_blocks
  matched_tokens
  effective_load
  body_mutation
```

body mutation 在原始 OpenAI body 中增加内部字段：

```json
{
  "mini_router": {
    "version": 1,
    "decision_id": "...",
    "token_ids": [1, 2, 3]
  }
}
```

除增加 `mini_router` 字段外，原始 OpenAI 参数保持不变。Gateway/Router 必须覆盖
客户端伪造的同名字段，不能合并客户端内容。token IDs 不能放在 HTTP header 中，
因为长 prompt 会超过 header size limit。

### 6.4 token-in adapter 到 vLLM

正常路径中，adapter：

- 删除 `mini_router`。
- 保留 `messages`、sampling 参数和 `stream` 等原始字段。
- 删除客户端提供的 `kv_transfer_params.prompt_token_ids`。
- 将 routing envelope 的 `token_ids` 写入 vLLM token-in 字段。
- 转发到本机固定 vLLM endpoint。

adapter 使用 backpressure 流式传输响应，不缓冲完整生成结果。

raw fallback 只接受来自受信 Gateway 的请求。adapter 删除 `mini_router` 以及客户端
可能提供的 token-in 字段，再把原始 OpenAI body 发给 vLLM；此时只由 vLLM tokenize
一次。

### 6.5 Gateway 返回 served endpoint metadata

Gateway 在上游 endpoint 确定后，通过 ext-proc response-stage metadata 返回：

```text
x-gateway-destination-endpoint-served = <ip:port>
```

Router 只用它提前释放对应 endpoint 的本地 reservation 和记录路由结果；不请求或读取
response body。metadata 缺失时 reservation 仍由 `reservation_ttl` 自动过期。

## 7. KV Event Index 与 Prefix 匹配

vLLM Worker 启用 prefix caching 和 KV event publishing：

```bash
vllm serve <model> \
  --enable-prefix-caching \
  --kv-events-config '{
    "enable_kv_cache_events": true,
    "publisher": "zmq",
    "endpoint": "tcp://*:5557",
    "replay_endpoint": "tcp://*:5558",
    "topic": "kv-events"
  }'
```

Router 消费并规范化：

```text
BlockStored
BlockRemoved
AllBlocksCleared
```

Prefix Index 维护：

```text
token prefix / block chain
  -> WorkerKey + local_generation ownership
```

处理规则：

- `BlockStored` 增加连续 block chain 和 ownership。
- `BlockRemoved` 删除对应 ownership。
- `AllBlocksCleared` 清除该 endpoint generation 的全部 ownership。
- duplicate 或旧 sequence 被忽略。
- sequence gap 出现时暂停该 endpoint 的 precise KV bonus。
- replay 能补齐 gap 时恢复 trusted 状态。
- replay 无法补齐时清除该 endpoint ownership，再从新事件重建。
- connection 重建或 sequence 回退时旧 generation 不能污染新状态。

查询使用 Renderer 产生的 `token_ids`，逐块计算当前 candidates 中每个 endpoint 的
最长连续 prefix 命中。非连续的散落 block 不计入 matched prefix。

## 8. 负载与路由评分

Router 为已经观察到的 endpoint 周期性读取 vLLM metrics：

- `vllm:num_requests_running`
- `vllm:num_requests_waiting`
- `vllm:kv_cache_usage_perc`
- metrics sample age

Router 不在响应路径中，因此无法维护真实的请求完成计数。为减轻 metrics 更新前的
瞬时流量扎堆，它只记录本副本最近选路产生的短期 reservation：

```text
effective_load = running + waiting + unexpired_local_reservations
```

每次选路增加一个 reservation；收到 served endpoint metadata 时提前释放，否则到达
`reservation_ttl` 后自动过期。它只是本地短期修正，不表示真实 in-flight，也不在
Router 副本之间同步。

项目只实现一种正式策略：

```text
cache_score = matched_prefix_tokens / token_ids.length
load_score = effective_load / max_concurrency

final_score =
    cache_weight * cache_score
  - load_weight * load_score
```

只对当前 request candidates 评分。KV 索引不可信的 endpoint 不获得 cache bonus；
metrics 不可用时使用已知 candidates 间的确定性 fallback。

## 9. 故障与降级

| 故障 | 行为 |
|---|---|
| candidate subset 缺失或为空 | Router 返回不可选路；Gateway 按部署策略 fail-open 或 503 |
| Router/EPP 不可用 | Gateway fail-open；vLLM 在 Worker 内 tokenize 一次 |
| Renderer 请求失败 | Router 不产生 token IDs，Gateway 按部署策略 fail-open |
| Renderer identity 不匹配 | Router readiness 失败 |
| 新 endpoint 尚未预热 | 不获得 KV bonus，使用负载或稳定 hash fallback |
| KV event gap 可 replay | 暂停该 endpoint KV bonus，replay 后恢复 |
| KV event gap 不可 replay | 清空 ownership，增加 generation，重新构建 |
| metrics 陈旧 | 不使用陈旧 load score |
| endpoint 不在当前 candidates | 绝不选择，即使本地仍有状态 |
| endpoint 超过 TTL 未出现 | 停止后台任务并清理全部本地状态 |
| token-in adapter 不可用 | Gateway 按自身已配置的重试或失败策略处理；Router 不接管响应路径 |
| vLLM streaming 已开始后失败 | 终止 stream，不透明重试 |

正常路径和 fail-open 路径都最多 tokenize 一次。

## 10. 安全边界

- Gateway 是唯一公网入口。
- Router/EPP、Renderer、token-in adapter、vLLM、metrics 和 KV event endpoint 只在
  内部网络暴露。
- Router 只接受 Gateway 提供的 candidate subset，不接受客户端自报 endpoint。
- Gateway/Router 必须删除客户端提供的 `mini_router` 字段。
- adapter 必须删除客户端提供的 vLLM token-in 字段，正常路径再写入 Router token IDs。
- 没有 routing envelope 的请求只有通过 Gateway workload identity 验证后才能进入
  raw fallback。
- Gateway 到 Router、Gateway 到 adapter 使用 mTLS 或等价 workload identity。
- adapter 到 vLLM 只使用 loopback 或 Unix Domain Socket。
- Router 和 adapter 不记录 prompt 或完整 token IDs。
- 对 body size、candidate count、token count 和内部请求延迟设置硬限制。

## 11. Rust 代码组织

```text
src/
  bin/
    router.rs
    adapter.rs
  epp/
    server.rs
    candidates.rs
    response.rs
  renderer/
    client.rs
  workers/
    endpoint.rs
    manager.rs
  kv/
    events.rs
    subscriber.rs
    index.rs
  routing/
    load.rs
    scorer.rs
    service.rs
  adapter/
    transform.rs
    proxy.rs
  lib.rs
  config.rs
  types.rs

tests/
  fixtures/
    events/
    renders/
  kind/
    cluster.yaml
    manifests/
    scenarios/
```

所有项目自有源码、部署文件和测试文件都使用单个小写英文单词作为文件名，不使用
下划线或连字符组合多个单词。目录同样使用单个小写单词。Cargo target 名可以继续是
`mini-router` 和 `vllm-token-in-adapter`，但 target 名不决定源文件名。

边界约束：

- `epp/server.rs` 只承载 Envoy ext-proc gRPC server，不实现 Gateway。
- `epp/candidates.rs` 将 request metadata 中的 endpoint subset 转换为 `CandidateSet`。
- `epp/response.rs` 生成 selected endpoint、dynamic metadata 和 body mutation。
- `workers` 只管理进程内 endpoint state 和后台任务生命周期。
- `kv` 不依赖 Gateway 或 Kubernetes 类型。
- `routing` 接收 `token_ids + CandidateSet + snapshots`，返回纯决策。
- `adapter` 只封装 vLLM 私有 token-in 字段和 response streaming。
- 代码中不引入 Kubernetes SDK。
- `tests/kind` 只通过容器镜像、Kubernetes manifests 和外部测试驱动操作集群，不能
  成为 Router 运行时依赖。

## 12. 配置与部署契约

### 12.1 不可变模型配置

Router 使用环境变量或普通配置文件读取：

```text
model_id
vllm_version
model_revision
tokenizer_revision
chat_template_digest
block_size
renderer_url
adapter_port
metrics_port
kv_event_port
kv_replay_port
endpoint_state_ttl
reservation_ttl
max_concurrency
cache_weight
load_weight
```

Kubernetes 中这些值可以来自 ConfigMap，但 Router 只看到普通环境变量或文件。

### 12.2 Kubernetes 示例资源

仓库只提供普通部署示例：

```text
deploy/k8s/
  router.yaml
  service.yaml
  pool.yaml
  route.yaml
  worker.yaml
```

示例表达：

- Router Deployment 中包含 Rust Router 和 vLLM Renderer sidecar。
- Worker Pod 中包含 Rust token-in adapter 和 vLLM container。
- InferencePool endpoint 指向 adapter port 8200。
- Gateway 调用 Router Service 的 EPP/ext-proc port。

这些 YAML 是集成示例，不引入项目自定义 Kubernetes API。

### 12.3 kind 测试部署

仓库提供一个可重复创建的 kind 测试集群。kind Node 是实际运行 kubelet 和 Pod 的容器
节点，不使用只伪造 Node/Pod API 对象的模拟器。

测试部署包含：

- 与生产相同的 Gateway 实现和 InferencePool API。
- Rust mini-router 和 CPU-only vLLM Renderer sidecar。
- 至少两个 Mock Worker Pod；每个 Pod 运行真实 Rust token-in adapter。
- Mock vLLM upstream，提供 OpenAI HTTP、metrics、KV event 和 replay 测试接口。
- 集群外测试驱动，通过 Gateway 发送请求并通过管理 API 编排测试场景。

默认 happy-path 使用真实 CPU-only vLLM Renderer 和固定的小型 text-only 模型配置；
Renderer 超时、错误响应等故障场景允许替换为确定性的 Mock Renderer。

生产二进制不实现“静态 candidates 模式”。纯 Rust 测试可以直接构造 `CandidateSet`
fixture，但所有跨组件 candidate 变化都必须由 kind 中的 Gateway/InferencePool 产生。

## 13. 可观测性

Router 至少暴露：

- endpoint picker latency
- candidate count 和 subset parse failure
- newly observed / expired endpoint count
- active endpoint state count
- renderer latency 和 failure count
- token count
- matched prefix blocks/tokens
- selected endpoint distribution
- fallback count by reason
- KV events received、duplicate 和 gap count
- replay success/failure
- per-endpoint event watermark、generation 和 trust state
- metrics sample age
- Prefix Index size

adapter 至少暴露：

- request count
- token-in request count
- raw fallback request count
- invalid envelope count
- upstream vLLM latency/error
- active streaming connections

日志不得记录 `messages` 或完整 token IDs。

## 14. 测试与 Benchmark

### 14.1 Rust 单元与组件测试

`cargo test` 不启动 Kubernetes，用于快速验证纯逻辑和协议边界：

- text-only request 校验和不支持请求拒绝。
- candidate subset 解析、去重、数量限制和缺失处理。
- scoring 绝不返回 `CandidateSet` 之外的 endpoint。
- `BlockStored`、`BlockRemoved` 和 `AllBlocksCleared`。
- duplicate、旧 sequence、gap、replay 和 generation 隔离。
- KV Index 不可信时 Least Load 降级。
- Renderer response 到内部 `token_ids` 的映射。
- adapter 删除内部字段并注入 vLLM token-in 参数。

这里的静态 candidates 只是在内存中构造的测试数据，不对应产品配置或运行模式。

### 14.2 默认 kind 无 GPU 集成测试

所有跨组件测试默认在 CI 创建的临时 kind 集群中运行。测试使用真实 Gateway、
InferencePool、Router、Renderer 和 adapter；只有需要 GPU 的 vLLM engine 由 Mock Worker
upstream 替代。

必须覆盖：

1. InferencePool/Gateway 把 candidate subset 交给 EPP。
2. EPP 返回的 endpoint header 与 dynamic metadata 一致。
3. EPP 标准 gRPC health 返回正确的 liveness、readiness 和 service 状态。
4. Gateway 应用 Router body mutation，请求到达选中的真实 adapter。
5. Renderer 产生的 `token_ids` 同时用于 Prefix Index 查询和 adapter envelope。
6. adapter 转发后的请求只包含受信 vLLM token-in 参数。
7. Mock Worker 发布 KV events 和 metrics 后，路由结果随 prefix ownership 与负载变化。
8. 新增 Worker Pod 后 Router 懒加载其 endpoint state。
9. Worker 从 InferencePool candidates 移除后立即不可选，TTL 后本地状态被清理。
10. event duplicate、gap、replay、断线重连和 generation 隔离。
11. KV 状态失效时的 Least Load 降级。
12. 非流式与 streaming response 穿过 adapter 和 Gateway，且不被缓冲或重组。
13. Gateway 返回 served endpoint metadata 后 reservation 被提前释放，EPP 不读取
    response body。
14. Router 不可用时 Gateway fail-open，Worker 路径只 tokenize 一次。
15. 启动两个 Router replicas 后，两者无需共享状态也能独立完成合法选路。

kind 测试不访问 Router 内部状态来伪造成员变化，也不要求 Router 拥有 Kubernetes
权限。测试结束后直接销毁整个临时集群。

### 14.3 真实 GPU Kubernetes 验证

发布前在带厂商 GPU driver 和 Device Plugin 的 Kubernetes 集群中，固定一个小模型启动
两个真实 vLLM Worker，验证：

1. Renderer 只被调用一次。
2. adapter 转发的 token IDs 与 Renderer 输出完全一致。
3. vLLM Worker 使用传入 token IDs，不重新 render prompt。
4. 共享 prefix 请求优先选择拥有对应 KV blocks 的 Worker。
5. 非流式和 streaming response 均保持 OpenAI-compatible。

GPU 验证是发布门禁，不要求普通开发机或每个 pull request 具备 GPU。kind 中伪造
`nvidia.com/gpu` capacity 不能替代这项验证。

### 14.4 Benchmark

只比较：

1. Least Load。
2. KV + Load Hybrid。

指标包括 prefix cache hit ratio、avoided prefill tokens、TTFT、throughput、load skew、
Renderer latency、Endpoint Picker latency、adapter overhead 和 KV Index recovery time。

## 15. 验收标准

### 15.1 项目边界

- 项目自研运行组件使用 Rust。
- 现成 Gateway 是唯一客户端 OpenAI API 入口。
- Router 只实现内部 EPP/ext-proc 协议，不代理响应流。
- Router 实现标准 gRPC health，并仅接收协议要求的 response-stage served metadata。
- Router 代码不依赖 Kubernetes SDK 或集群 API。
- Kubernetes membership 只通过 request-scoped candidate subset 进入 Router。
- 每个 Worker 前有一个 Rust token-in adapter。
- 只支持一个固定模型和 text-only `/v1/chat/completions`。

### 15.2 Render Once

- 正常请求只调用一次 vLLM Renderer。
- Router 使用 Renderer 的 `token_ids` 计算 prefix overlap。
- 同一份 `token_ids` 经 body mutation 和 adapter 传入 vLLM。
- vLLM Worker 不重复执行 chat rendering 或 tokenization。
- vLLM 私有 token-in 字段只存在于 adapter 内部。
- fail-open 路径由 Worker tokenize 一次，不产生双重 tokenization。

### 15.3 Candidate 与本地状态

- Router 只能返回当前 EPP request candidate subset 中的 endpoint。
- 新 endpoint 可以被懒加载并在状态就绪前安全 fallback。
- 消失 endpoint 不会继续被选择，TTL 后本地状态被回收。
- connection 重建和 sequence 回退不会让旧 generation 污染新状态。
- 多个 Router 副本不需要共享或复制 KV Index。
- 生产二进制不存在静态 candidates 运行模式。

### 15.4 KV-aware routing

- Router 消费真实 vLLM KV events。
- Prefix Index 能维护连续 block ownership。
- event duplicate 和 sequence gap 不会产生错误 cache bonus。
- Hybrid 同时考虑 prefix overlap 和 Worker load。
- KV 状态不可用时安全降级。

## 16. 完成定义

以下条件全部满足时，本项目一次性交付完成：

1. Gateway 能把 text-only chat request 和 candidate endpoint subset 交给 Rust EPP。
2. Router 调用 CPU-only vLLM Renderer，只生成一次 `token_ids`。
3. Router 为观察到的 endpoints 消费 KV events 和 metrics。
4. Router 只在当前 candidates 内基于 prefix overlap 与负载选择 endpoint。
5. Gateway 应用 selected endpoint 和 Router body mutation。
6. Rust token-in adapter 将相同 token IDs 传给本机 vLLM，且 vLLM 不重复 tokenize。
7. 非流式和 streaming response 能经 adapter 与 Gateway 正确透传。
8. Router 不需要 Kubernetes API、分布式协调服务或跨副本状态同步。
9. `cargo test` 与默认 kind 无 GPU 集成测试能验证核心正确性和完整协议链路。
10. 发布前真实 GPU Kubernetes 验证和 Benchmark 能验证 vLLM 兼容性与路由收益。
