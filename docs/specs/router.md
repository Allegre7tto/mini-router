# KV-aware Router

本规格定义选路数据面。跨节点 KV 传输，以及 Local / Fetch / Recompute 的执行协调，见 [2part.md](2part.md)。

Phase 3 的可选 GPU KV Staging 见 [Operator.md](Operator.md)。它只扩展传输 Runtime 的 Worker 数据路径，不依赖 Router，也不改变本规格的协议、评分或验收条件。

## 1. 目标与边界

Router 用 Rust 实现，只解决单模型文本推理的请求选路。它在 Gateway 给出的 candidate endpoint 中，选择期望 prefill 完成时间最短的 Worker。

Router 不传输、复制或持久化 KV Tensor，也不在本地复用、远程拉取和重新计算之间做决策。它选择的是 adapter endpoint，不是 [2part.md](2part.md) 里持有 block 并执行计算的 Worker。

Router 不暴露客户端推理 API，不实现集群控制面，也不调用 Kubernetes API。多个副本各自维护本地状态，不跨副本同步。状态差异只影响命中收益，不影响推理正确性。

## 2. 范围

### 2.1 包含

- 一个固定模型，以及多个相同 revision 的 vLLM Worker。
- text-only `/v1/chat/completions`，包括非流式和 streaming。
- 作为内部 Endpoint Picker，遵循 Gateway API Inference Extension 的 Endpoint Picker Protocol。
- 每次选路使用 Gateway 提供的 candidate endpoint subset。
- 选路前只 render 一次。同一份 `token_ids` 同时用于 prefix 匹配和 vLLM 推理。
- 消费 vLLM KV cache events，在内存中维护 endpoint cache view。
- 一种正式策略：最短 prefill 完成时间。没有速率样本时，在最长命中中选最短队列。
- 默认集成测试运行在无 GPU 的 kind 集群中。

### 2.2 不包含

- Kubernetes client、Informer、Controller、Operator、自定义 CRD 和 Leader Election。
- Worker 自注册、Lease、分布式成员管理和 Router 状态复制。
- 多模型、动态加载、多模态、LoRA、tool calling、reasoning parser 和 structured output。
- `/v1/completions`、Responses API 和其他 OpenAI endpoint。
- Prefill/Decode disaggregation。
- KV Tensor 的传输、复制，以及 CPU、SSD 或远端放置。
- autoscaling、请求迁移、stream resume 和语义路由。
- 自研 Gateway、鉴权、限流和公网 API server。

超出范围的请求由 Gateway 或 Router 拒绝，不得静默变成不精确的 KV-aware 行为。

## 3. 架构

```text
Client -> Inference Gateway
              |  ext-proc/EPP
              |  body + candidate endpoint subset
              v
           Router
              |-- Renderer sidecar -> token_ids
              |-- Endpoint State
              |-- KV Event Index
              |-- Prefix Index
              |-- Load View
              `-- Scorer
              |
              | selected endpoint + body mutation
              v
         Inference Gateway
              |
              v
         token-in adapter -> loopback vLLM
              |
              |-- KV events -> Router replicas
              |-- metrics ----> Router replicas
              `-- response ---> Gateway -> Client
```

Kubernetes、InferencePool 和 Gateway 负责 membership、readiness、Pod 生命周期和扩缩容。这些状态不进入 Router。kind 测试也不得向 Router 注入专用的成员管理接口。

Gateway 是外部依赖，也是唯一客户端入口。生产和 kind 使用同一类 Gateway；candidate 都来自 EPP request metadata。Gateway 负责：

- 暴露 `/v1/chat/completions`，以及鉴权、限流、TLS 和 body 大小限制。
- 缓冲用于选路的 request body，并提供 `x-gateway-destination-endpoint-subset`。
- 调用 Router，取得目标 endpoint 和 body mutation。
- 把修改后的请求发到目标 adapter，再向客户端透传 vLLM 响应。

Router 不持有客户端 streaming connection，也不代理生成响应。为符合 Endpoint Picker Protocol，ext-proc 可以在响应阶段接收 `x-gateway-destination-endpoint-served`，但 response body processing 必须关闭。

选路链路是：

```text
OpenAI request -> Renderer -> token_ids -> routing -> adapter -> vLLM
```

每个 Router Pod 旁有一个 CPU-only Renderer sidecar。它加载模型配置、tokenizer 和 chat template，不加载权重，也不使用 GPU。Router 不重写 tokenizer、chat template 或 renderer。

```bash
vllm launch render <model> --host 127.0.0.1 --port 8100
```

每个 Worker Pod 前有一个同 Pod 的 token-in adapter。InferencePool 暴露的是 adapter endpoint，不是 vLLM endpoint。adapter 校验内部 envelope，去掉客户端伪造的 token-in 参数，写入当前 vLLM 版本要求的字段，再把响应按字节流转发给 loopback vLLM。它不解析或重组 SSE。

## 4. Candidate 与本地状态

### 4.1 请求级边界

Gateway 在 ext-proc metadata 中提供：

```text
x-gateway-destination-endpoint-subset
```

Router 按 Gateway API Inference Extension 解析该字段。字段缺失、为空，或其中没有可选 endpoint 时，Router 返回不可选路。Gateway 按其部署策略 fail-open 或返回 503。

选中的目标必须同时写入 `x-gateway-destination-endpoint` 和 Endpoint Picker Protocol 要求的 dynamic metadata，两处值必须一致。

Router 只能选择当前 subset 中的 endpoint。本地仍有状态、但不在 subset 中的 endpoint 不得被选择。

### 4.2 端口

candidate endpoint 指向同 Pod 的 adapter：

```text
adapter    <pod-ip>:8200
vLLM HTTP  <pod-ip>:8000
metrics    <pod-ip>:8000/metrics
KV events  tcp://<pod-ip>:5557
KV replay  tcp://<pod-ip>:5558
```

Router 用 candidate 的 host 和上述固定端口推导 metrics、event 和 replay 地址，不查询 Pod、Service 或 EndpointSlice。内部键是：

```text
WorkerKey = canonical(candidate endpoint)
```

### 4.3 懒加载与清理

每次收到 candidate subset 时：

```text
observe(candidates)
  ├── 已知 endpoint：更新 last_seen
  └── 新 endpoint：创建 generation，启动 metrics，连接 KV events，并尝试 replay
```

新 endpoint 的后台状态尚未就绪时可以参与选路，但命中 token 数视为 0。完成时间相同，或都没有速率样本且命中也相同时，用 `request_id + endpoint` 的稳定 hash 打破平局。

当前请求不包含某 endpoint 时，即使本地还有索引也不得选择它。后台 janitor 删除超过 `endpoint_state_ttl` 未再出现的状态，并停止其 metrics 和 KV 连接，清除 cache ownership、load 和 event watermark。

event 连接重建、sequence 回退，或无法修复的 gap，都会增加本进程的 generation，并在接受新事件前清除旧 ownership。该 generation 只隔离单个进程内的状态，不是分布式身份。

## 5. 请求协议

### 5.1 Gateway 到 Router

```text
RoutingRequest
  request_id
  model
  openai_chat_body
  candidate_endpoints[]
  received_at
```

Router 拒绝以下请求：`model` 不是配置的唯一模型，`messages` 缺失或含有非文本 content，body 或预计 token 数超限，candidate 格式无效或数量超限。

### 5.2 Router 到 Renderer

Router 去掉客户端保留字段后，把 chat completion body 发给本机 Renderer：

```text
POST /v1/chat/completions/render
```

它只读取 `token_ids`。Router 内部、routing envelope 和 KV 查询都使用这个字段名。

Renderer、adapter 和全部 Worker 必须使用同一个受支持的 vLLM 版本。Renderer 与 Worker 还必须具有相同的 model revision、tokenizer revision、chat template digest 和 special token 配置。这些值来自同一份不可变部署配置。identity 不匹配时，Router 不得进入 ready。

### 5.3 选路结果

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

body mutation 只增加内部字段，其余 OpenAI 参数保持不变：

```json
{
  "mini_router": {
    "version": 1,
    "decision_id": "...",
    "token_ids": [1, 2, 3]
  }
}
```

`mini_router` 是 envelope 字段名。Gateway 和 Router 必须覆盖客户端伪造的同名字段，不得与客户端内容合并。token IDs 不得放在 HTTP header 中。

### 5.4 adapter 到 vLLM

有 envelope 时，adapter 校验 envelope、模型 ID 和 token 数，然后：

- 删除 `mini_router`。
- 保留 `messages`、sampling 参数和 `stream`。
- 删除客户端提供的 `kv_transfer_params.prompt_token_ids`。
- 把 envelope 中的 `token_ids` 写入 vLLM token-in 字段。
- 转发到本机固定的 vLLM endpoint。

当前 vLLM 字段是：

```json
{
  "kv_transfer_params": {
    "prompt_token_ids": [1, 2, 3]
  }
}
```

该字段是 vLLM 的实验接口，只能由 adapter 读写。Gateway、Router core 和 envelope 都不得依赖这个字段名。

没有 envelope 的请求只有通过受信 Gateway 的 workload identity 校验后，才能进入 raw fallback。adapter 删除 `mini_router` 和客户端 token-in 字段，再把原始 OpenAI body 发给 vLLM，由 vLLM tokenize 一次。

adapter 用 backpressure 流转发响应，不缓冲完整生成结果，也不重组 SSE。

### 5.5 served endpoint

上游 endpoint 确定后，Gateway 通过 ext-proc response metadata 返回：

```text
x-gateway-destination-endpoint-served = <ip:port>
```

Router 只用它提前释放对应 reservation，并记录选路结果。它不读取 response body。metadata 缺失时，reservation 在 `reservation_ttl` 后过期。

## 6. KV Index

Worker 启用 prefix caching 和 KV events：

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

Router 把事件规范为 `BlockStored`、`BlockRemoved` 和 `AllBlocksCleared`。Prefix Index 记录：

```text
token prefix / block chain -> WorkerKey + local_generation
```

- `BlockStored` 增加连续 block chain 及其 ownership。
- `BlockRemoved` 删除对应 ownership。
- `AllBlocksCleared` 清除该 endpoint generation 的全部 ownership。
- 重复或更旧的 sequence 被忽略。
- sequence gap 出现时，该 endpoint 的命中 token 数视为 0，直到 replay 恢复。
- replay 补齐 gap 后恢复 trusted。无法补齐时清除 ownership，再从新事件重建。
- 连接重建或 sequence 回退时，旧 generation 不得进入新状态。

查询使用 Renderer 产生的 `token_ids`，只计算当前 candidates 中每个 endpoint 的最长连续 prefix。不连续的 block 不计入命中。

## 7. 调度

正式策略是最短 prefill 完成时间。不估计生成长度，也不用权重把命中比例和负载加在一起。只在当前 candidates 中选择。

对 endpoint \(i\)，\(n\) 是本次 prompt 的 token 数，\(h_i\) 是可信的最长连续命中。索引不可信、尚未预热或存在未修复 gap 时，\(h_i = 0\)。\(q_i\) 是尚未完成的 prefill token 数，\(r_i\) 是最近一次成功的 prefill 速率，单位是 token/s。没有成功样本就不估计，也不平滑。

\[
Q_i = \frac{q_i}{r_i}, \qquad L_i = Q_i + \frac{n - h_i}{r_i}
\]

选择 \(L_i\) 最小的 endpoint。Router 不传输 KV，因此没有拉取项；拉取和重算的比较见 [2part.md](2part.md)。

\(q_i\) 优先用 Worker 报告的排队 prefill token 数。没有这份新鲜样本时，\(q_i\) 只计本副本未过期 reservation 的未命中 token 数。请求个数不得换算成 token。每次选路按当时的 \(n - h_i\) 增加 reservation；收到 served metadata 时释放，否则在 `reservation_ttl` 后过期。reservation 不在副本之间同步。陈旧的 \(q_i\) 或 \(r_i\) 视为不存在。

若没有任何 endpoint 具有 \(r_i\)，则在 \(h_i\) 最大的 endpoint 中选择等待请求最少的。等待请求数只用于这次退让，不进入 \(L_i\)。

## 8. 故障

| 故障 | 行为 |
|---|---|
| candidate subset 缺失或为空 | 返回不可选路；Gateway fail-open 或返回 503 |
| Router 或 EPP 不可用 | Gateway fail-open；Worker 内 tokenize 一次 |
| Renderer 失败 | Router 不产生 token IDs；Gateway 按其策略 fail-open |
| Renderer identity 不匹配 | Router 不进入 ready |
| endpoint 尚未预热 | \(h_i = 0\)；没有 \(r_i\) 时不参与完成时间比较 |
| KV gap 可以 replay | \(h_i = 0\)，replay 后恢复命中 |
| KV gap 不能 replay | 清空 ownership，增加 generation，再重建 |
| metrics 陈旧 | 不使用该样本的 \(q_i\) 和 \(r_i\) |
| endpoint 不在当前 candidates | 不得选择 |
| endpoint 超过 TTL 未出现 | 停止后台任务并清除本地状态 |
| adapter 不可用 | 由 Gateway 已配置的重试或失败策略处理；Router 不接管响应 |
| streaming 开始后失败 | 终止 stream，不透明重试 |

正常路径和 fail-open 路径都最多 tokenize 一次。

## 9. 安全

- Gateway 是唯一公网入口。Router、Renderer、adapter、vLLM、metrics 和 KV event 只暴露在内部网络。
- Router 只接受 Gateway 给出的 candidate，不接受客户端自报的 endpoint。
- Gateway 和 Router 必须删除客户端提供的 `mini_router`。
- adapter 必须删除客户端提供的 vLLM token-in 字段；正常路径再写入 Router 的 token IDs。
- 没有 envelope 的请求，只有通过 Gateway workload identity 校验后才能 raw fallback。
- Gateway 到 Router、Gateway 到 adapter 使用 mTLS 或等价的 workload identity。
- adapter 到 vLLM 只使用 loopback 或 Unix domain socket。
- Router 和 adapter 不记录 prompt 或完整 token IDs。
- body size、candidate count、token count 和内部请求延迟都有硬限制。

## 10. 配置与部署

Router 从环境变量或普通配置文件读取。Kubernetes 可以把这些值放在 ConfigMap 中，但 Router 只看到环境变量或文件：

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
```

`deploy/k8s/` 只提供普通部署示例，不定义自定义 Kubernetes API。示例必须表达：

- Router Deployment 包含 Router 和 Renderer sidecar。
- Worker Pod 包含 adapter 和 vLLM。
- InferencePool 指向 adapter 的 8200 端口。
- Gateway 调用 Router 的 EPP/ext-proc 端口。

kind 测试使用真实 kubelet，不用只伪造 Node 或 Pod 对象的模拟器。测试包含与生产相同的 Gateway 和 InferencePool、Router、CPU-only Renderer，以及至少两个运行真实 adapter 的 Mock Worker。Mock upstream 提供 OpenAI HTTP、metrics、KV event 和 replay。集群外的测试驱动通过 Gateway 发请求。

默认成功路径使用真实 Renderer 和固定的小型 text-only 模型。Renderer 超时和错误响应可以用确定性的 Mock Renderer。生产二进制没有静态 candidates 模式。纯 Rust 测试可以在内存中构造 `CandidateSet`；跨组件的 candidate 变化必须来自 Gateway 或 InferencePool。

## 11. 模块边界

```text
src/epp/        ext-proc、candidate 解析、选路响应
src/renderer/   Renderer client
src/workers/    进程内 endpoint 状态和后台任务
src/kv/         event、replay、prefix index
src/routing/    load、评分和选路
src/adapter/    vLLM token-in 字段和响应转发
```

- `epp` 不实现 Gateway。
- `workers` 只管理本进程状态，不查询 Kubernetes。
- `kv` 不依赖 Gateway 或 Kubernetes 类型。
- `routing` 接收 `token_ids`、`CandidateSet` 和状态快照，返回纯决策。
- `adapter` 是唯一读写 vLLM 私有 token-in 字段的模块。
- 代码不引入 Kubernetes SDK。
- kind 测试通过镜像、manifest 和外部驱动操作集群，不是 Router 的运行时依赖。

Router 不依赖 [2part.md](2part.md) 的 Transfer Engine。传输实现也不得依赖 `epp`、`kv` 或 `routing`。

## 12. 可观测性

Router 至少暴露：选路延迟，candidate 数量和解析失败，新观察到的及过期 endpoint 数，活跃 endpoint 状态数，Renderer 延迟和失败数，token 数，命中的 prefix block 与 token 数，选中 endpoint 的分布，按原因分类的 fallback 数，KV event、重复和 gap 数，replay 成败，每个 endpoint 的 watermark、generation 和 trust，metrics sample age，以及 Prefix Index 大小。

adapter 至少暴露：请求数、token-in 请求数、raw fallback 数、无效 envelope 数、上游延迟和错误数、活跃 streaming 连接数。

日志不得记录 `messages` 或完整 token IDs。

## 13. 测试

### 13.1 组件测试

`cargo test` 不启动 Kubernetes，覆盖：

- text-only 校验，以及超出范围的请求被拒绝。
- candidate 解析、去重、数量限制和缺失处理。
- 评分不返回 `CandidateSet` 之外的 endpoint。
- `BlockStored`、`BlockRemoved`、`AllBlocksCleared`。
- 重复事件、旧 sequence、gap、replay 和 generation 隔离。
- KV Index 不可信时 \(h_i = 0\)，不把旧命中算进 \(L_i\)。
- Renderer 响应到 `token_ids` 的映射。
- adapter 删除内部字段并注入 vLLM token-in 参数。

内存中的 `CandidateSet` 只是测试数据，不是运行模式。

### 13.2 kind 集成测试

默认跨组件测试在临时 kind 集群中运行，使用真实 Gateway、InferencePool、Router、Renderer 和 adapter。只有需要 GPU 的 vLLM engine 由 Mock upstream 代替。必须覆盖：

1. InferencePool 和 Gateway 把 candidate subset 交给 EPP。
2. endpoint header 与 dynamic metadata 一致。
3. EPP gRPC health 正确反映 liveness、readiness 和 service 状态。
4. Gateway 应用 body mutation，请求到达选中的真实 adapter。
5. 同一份 `token_ids` 同时用于 Prefix Index 和 adapter envelope。
6. adapter 转发的请求只含受信的 vLLM token-in 参数。
7. Mock Worker 发布 events 和 metrics 后，选路随 ownership 和负载变化。
8. 新 Worker 被懒加载。
9. Worker 离开 candidates 后立即不可选，TTL 后本地状态被清除。
10. event 重复、gap、replay、断线重连和 generation 隔离。
11. KV 状态失效时该 endpoint 的 \(h_i\) 为 0，选路不再使用失效前的命中。
12. 非流式和 streaming 响应穿过 adapter 与 Gateway，且不被缓冲或重组。
13. served metadata 提前释放 reservation；EPP 不读取 response body。
14. Router 不可用时 Gateway fail-open，Worker 路径只 tokenize 一次。
15. 两个 Router 副本不共享状态，也能各自完成合法选路。

测试不读取 Router 内部状态来伪造成员变化，也不授予 Router Kubernetes 权限。结束后销毁临时集群。

### 13.3 GPU 验证

发布前，在带厂商 GPU driver 和 Device Plugin 的集群上，用一个小模型和两个真实 vLLM Worker 验证：

1. Renderer 只被调用一次。
2. adapter 转发的 token IDs 与 Renderer 输出一致。
3. Worker 使用传入的 token IDs，不重新 render。
4. 有速率样本时选择 \(L_i\) 最小的 Worker，而不是固定选择命中最长的 Worker。
5. 非流式和 streaming 响应保持 OpenAI-compatible。

这是发布门禁，不是每个 pull request 的要求。在 kind 里伪造 `nvidia.com/gpu` 不能代替它。

### 13.4 Benchmark

比较最短队列和最短完成时间。指标包括 prefix cache hit ratio、避免的 prefill tokens、TTFT、吞吐、负载偏斜、Renderer 延迟、Endpoint Picker 延迟、adapter 开销和 KV Index 恢复时间。

## 14. 完成定义

以下全部满足时，本规格交付完成：

1. Gateway 把 text-only chat request 和 candidate subset 交给 Router。
2. Router 只调用一次 Renderer，并用同一份 `token_ids` 做 prefix 匹配和 body mutation。
3. adapter 把这份 token IDs 传给本机 vLLM，Worker 不重复 tokenize。
4. Router 只在当前 candidates 内，按 \(L_i\) 选 endpoint；没有速率样本时，在最长命中中选最短队列。
5. 非流式和 streaming 响应经 adapter 与 Gateway 透传。
6. Router 不调用 Kubernetes API，也不在副本之间同步状态。
7. Router 不传输 KV，也不产生 Fetch 或 Recompute 计划。
8. `cargo test` 和默认 kind 集成测试通过。
9. 发布前的 GPU 验证和 Benchmark 确认 vLLM 兼容性和路由收益。
