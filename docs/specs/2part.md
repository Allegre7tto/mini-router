# KV Cache 分布式传输与执行 Runtime

本规格定义跨节点 KV block 的传输，以及建立在传输之上的执行协调。只选路、不移动 KV 的数据面见 [router.md](router.md)。

本规格的范围与验收针对 CPU-only 的 Phase 1/2。可选的 Phase 3 使用 cuTile Rust 优化 GPU KV Staging，见 [Operator.md](Operator.md)；它不增加 Phase 1/2 的 GPU 依赖或验收要求。

## 1. 目标

推理请求依赖的 KV block 可能在其他节点上，各节点负载也不相同。完成时间由三件事共同决定：数据如何移动，计算在哪个节点执行，传输和计算如何按依赖衔接。

本规格分两层交付，都用 Rust 实现：

1. **Transfer Engine**：可独立评测的异步内存传输库。它不理解 LLM、KV Cache 或 vLLM。
2. **Execution Runtime**：在本地复用、远程拉取和重新计算之间选择计划，并执行该计划。

分层参考 Mooncake 的传输层与上层分离，但不复刻 Mooncake，也不建设 KV 存储或推理平台。

只比较本地完成时间、且不传输 KV 的策略见 [router.md](router.md)。它是对照基线，不是本规格的实现范围。

## 2. 范围

### 2.1 包含

Phase 1，Transfer Engine：

- 内存注册、异步读写、完成通知、并发限制和背压。
- 必须实现 TCP。RDMA 是同一接口下的可选后端。
- 可重复的传输微基准。

Phase 2，Execution Runtime：

- 单控制节点上的内存 Block Directory 和 Worker 状态。
- 在 Local、Remote Fetch、Recompute 中选择 Worker 和策略。
- 通过 Phase 1 异步搬移 block，并按依赖启动计算。
- 模拟 Worker 上的端到端基准。

### 2.2 不包含

- 推理服务、tokenizer、sampling 和 attention kernel。
- 对象存储、SSD/NVMe 分层和缓存数据库。
- 共识、多副本强一致元数据和高可用控制面。
- Kubernetes GPU 调度、autoscaling 和多租户控制面。
- Prefill/Decode disaggregation。
- GPUDirect RDMA、GPU 内存注册和设备拓扑优化。
- 多模型、多版本下的全部 KV 布局兼容。
- 从 vLLM 或其他引擎导入、导出真实 KV Tensor。

真实引擎集成不是完成条件。将来接入时，必须已有明确的导出/导入路径，并校验模型版本、并行分片、layout 和 dtype。本规格不定义该路径。

## 3. 架构

```text
Inference Requests
        |
        v
Execution Coordinator
  request state, dependencies, failure
        |
        v
Cache-aware Scheduler
  location, load, fetch vs recompute
        |
        v
KV Block Directory + Worker State
        |
        v
Async Transfer Coordinator
        |
======== component boundary ========
        |
Transfer Engine
  registered memory, TCP / optional RDMA,
  async transfer, completion, flow control
        |
   Worker A <--> Worker B
```

这是组件职责，不是推理服务的部署图。此处的 Worker 是持有 block 并执行计算的节点，不是 [router.md](router.md) 里的 candidate endpoint。

边界：

- Transfer Engine 不依赖 Router、KV event、EPP 或 vLLM 类型。
- Runtime 只依赖传输 trait，不依赖 TCP 或 RDMA 的实现。
- Scheduler 只产出执行计划，不 pin buffer，不提交传输，不启动计算。
- Router 不调用 Transfer Engine，也不产出 Fetch 或 Recompute 计划。
- 本规格中的 block 是定长、不可变、不透明的字节缓冲。写完并发布后才可读。GPU 内存布局不在 Phase 1/2 范围内，由 [Operator.md](Operator.md) 独立定义。

## 4. Transfer Engine

### 4.1 接口

| 模块 | 要求 |
|---|---|
| `MemoryRegion` | 注册可传输内存，记录长度、权限和生命周期。越界访问失败。在途传输仍可能访问时不得释放。 |
| `Transport` | 统一读写接口。TCP 是必须后端，RDMA 是可选后端。上层不绑定某一种机制。 |
| `TransferRequest` | 描述方向、源、目标、偏移和长度。异步提交并返回 handle。 |
| `Completion` | 终态只有成功、失败、超时、取消。每个请求只交付一次终态，之后的重复通知必须忽略。 |
| 流控 | 批量提交、连接复用、并发上限和背压。超过上限时等待或失败，不得无界排队。 |

```rust
let region = engine.register_memory(buffer, permissions)?;
let handle = engine.read(remote, remote_offset, region, local_offset, len).await?;
handle.wait().await?;
```

### 4.2 语义

TCP 上的远端 Read/Write 是请求—响应仿真，不是 RDMA 单边读写。接口和错误必须区分这两类操作。调用方不得假定 TCP 后端具有硬件单边语义。

远端描述符在执行前必须通过身份、权限、范围和生命周期校验。校验失败的请求不得开始传输。

### 4.3 生命周期

- 注册期间，调用方保留 buffer 所有权，引擎持有访问权。
- 仍有在途传输时，`deregister` 必须等待其结束，或返回错误并保持注册有效。等待期间不得把该内存交给其他所有者。
- 失败、超时、取消和断连都不得留下仍可能被写入的已发布状态。
- 部分传输不是成功。未完整写入的目标区域不得发布。

这些规则同时约束 TCP 和 RDMA。没有 DMA 时，在途请求结束前同样不得复用或释放该区域。

### 4.4 RDMA

RDMA 后端实现 memory registration、queue pair、work request 和 completion queue，并遵守 4.3。没有 RDMA 硬件时 Phase 1 仍然完成；不得用桩实现假装单边读写成功。

有硬件时，基准必须在相同负载下比较 TCP 与 RDMA 的带宽、P50/P99 和 CPU 占用。

## 5. Execution Runtime

### 5.1 组件

| 组件 | 职责 | 边界 |
|---|---|---|
| `KV Block Directory` | 查询 block 是否存在、在哪个 Worker、版本和可用状态 | 单控制节点，只存内存元数据，不存 block 字节 |
| `Cache-aware Scheduler` | 选择 Worker，以及 Local / Fetch / Recompute | 只产出计划 |
| `Async Transfer Coordinator` | 把缺失 block 变成异步传输，跟踪依赖和完成 | 只调用 Phase 1 的接口 |
| `Execution Coordinator` | 请求状态机、资源预约、启动条件和失败处理 | 不实现模型推理 |

控制节点重启后 Directory 为空，之后只反映重新发布的 block。不持久化元数据。

### 5.2 Block

`BlockId` 包含模型版本、layout、dtype、分片标识和块序号。缺任何一项都不可比较。不兼容的 block 视为不存在，不得拉取，也不得算作本地命中。

v1 只支持固定大小的不可变 block，不支持覆盖写。写完并校验后才发布；发布前，目标 buffer 对调度和计算都不可见。

不实现通用缓存一致性，也不实现多副本协议。同一 block 可以出现在多个 Worker 上，每一份都是独立发布的完整副本。

### 5.3 执行计划

```rust
enum ExecutionPlan {
    UseLocal { worker: WorkerId },
    FetchThenExecute {
        source: WorkerId,
        destination: WorkerId,
        blocks: Vec<BlockId>,
    },
    RecomputeThenExecute { worker: WorkerId },
}
```

`UseLocal` 要求目标 Worker 已有全部所需 block。`FetchThenExecute` 的 `blocks` 只包含目标缺失的部分。`RecomputeThenExecute` 不依赖远程 block。

### 5.4 调度

调度选择期望 prefill 完成时间最短的可行计划。不估计生成长度，也不用权重把命中和负载加在一起。速率取最近一次成功样本，不平滑。没有样本的项不可用。

符号与 [router.md](router.md) 相同：\(n\) 是 prompt token 数，\(h_i\) 是 Worker \(i\) 上可信的最长连续命中，\(q_i\) 是尚未完成的 prefill token 数，\(r_i\) 是 prefill 速率。另加源 Worker \(s\)：\(e_{si}\) 是 \(s\) 比 \(i\) 多出的兼容前缀 token 数，\(\delta_{si}\) 和 \(g_{si}\) 是该路径最近一次成功传输的往返时间和吞吐，\(b_{si}\) 是在途字节加上本次要搬的字节。

\[
Q_i = \frac{q_i}{r_i}, \qquad L_i = Q_i + \frac{n - h_i}{r_i}
\]

\[
F_{si} = \max\left(Q_i,\ \delta_{si} + \frac{b_{si}}{g_{si}}\right) + \frac{n - h_i - e_{si}}{r_i}
\]

\(L_i\) 是不拉取时的完成时间。\(h_i = n\) 时它只剩排队，计划为 `UseLocal`；否则为 `RecomputeThenExecute`。\(e_{si} > 0\) 且 \(\delta_{si}\)、\(g_{si}\) 都有样本时，才计算 \(F_{si}\)，计划为 `FetchThenExecute`。选择这些时间中的最小值。

\(F_{si}\) 在以下情况不可行：block 不兼容，目录项过期，源不可用，或目标空闲块放不下要搬入的 block。空闲块由 Worker 报告，不用占用率阈值。在途字节变大时 \(F_{si}\) 变大，不另设源端 pin 上限。

传输没有被排队盖住时，同一 Worker 上拉取优于重算，当且仅当

\[
\delta_{si} + \frac{b_{si}}{g_{si}} < \frac{e_{si}}{r_i}
\]

排队更长时，正的 \(e_{si}\) 会直接使 \(F_{si} < L_i\)。这个 \(\max\) 只表示传输可以和队列重叠，不表示可以提前使用未就绪的 block。

没有任何 \(r_i\) 时不拉取。在 \(h_i\) 最大的 Worker 中选择等待请求最少的；仍然不可行则失败。不得用过期目录项构造 Fetch 计划。

### 5.5 状态机

```text
Admitted
  -> Choose plan / reserve
  -> Pin source + reserve destination
  -> Submit transfers
  -> Wait for required dependencies
  -> Validate + publish
  -> Dispatch compute
  -> Release pins / update directory
```

`UseLocal` 和 `RecomputeThenExecute` 跳过传输。重算产生的 block 同样只在写完后发布。

不变量：

- 源 block 在传输结束前不得被淘汰或复用。结束包括成功、失败和取消。
- 目标 block 在校验通过并发布前，不得被计算使用，也不得被其他请求当作命中。
- 只等待当前计算实际依赖的传输。与这些 block 无关的任务可以继续执行。
- 不得把未就绪的 block 交给计算。
- 预取和在途传输不得超过并发上限和缓冲上限。达到上限后停止新的预取。

失败时先释放尚未发布的预约，再按下面的规则处理。不无限重试。

- 提交前源不可用、pin 失败或元数据与源不一致：改选其他可行方案；没有则改为 Recompute。
- 传输已经提交后失败、超时，或源在传输中退出：不发布不完整数据，不再次 Fetch，改为一次 Recompute。优先原目标 Worker；它已不可用时选择另一个有容量的 Worker。都不可用则请求失败。
- 目标 Worker 在计算前失败：中止请求，不发布半成品。
- 计算失败：不发布本次产生的 block。此前已经发布的副本保持有效，直到该 Worker 退出并被清除。

### 5.6 重叠

基线是：依赖的传输全部完成后，再启动对应计算。

唯一允许的优化，是执行与未就绪 block 没有依赖的其他任务，或提前传输已经 pin 住的后续 block。不能证明无依赖，就不得并行。重叠不改变 5.5 的失败语义。

### 5.7 Phase 3 集成边界

GPU Staging 是 Worker 的可选能力。启用后，Async Transfer Coordinator 通过独立的 Worker 接口等待源端准备和目标端恢复；Transfer Engine 仍只处理 Host `MemoryRegion`，不依赖 GPU 类型。GPU 显存分配不属于 Phase 1 的 GPU 内存注册或 GPUDirect 支持。

Fetch 在原状态机内展开为 Pack、D2H、网络传输、H2D、Unpack 和验证，完成后才能发布。取消请求不代表设备操作结束，必须安全排空后再释放或复用资源。Worker 内部可按完整 block 分 Chunk 重叠执行，具体约束见 [Operator.md](Operator.md)，不改变 5.5 的失败规则和 5.6 的计算依赖。

Block 兼容性、执行计划和 5.4 的评分公式保持不变。GPU 数据准备成本与预测误差单独报告，不改变原网络指标的含义。

## 6. 评测与验收

每次结果都记录硬件、网络拓扑、块大小、连接数和并发度。模拟结果必须标明为模拟，不得写成 TTFT、TPOT 或 GPU 利用率。

### 6.1 Phase 1

必须满足：

- 两个进程经 TCP 正确传输测试字节，覆盖小于、等于和大于一个 block 的长度。
- 并发传输、越界、超时、断连、部分传输、重复完成、并发关闭和在途释放，都符合第 4 节。
- 在途期间释放内存不会成功。
- 基准报告有效带宽、operations/s、P50/P99、CPU 时间和峰值内存。

同一接口必须能在两台机器上运行同一套测试。没有第二台机器时，Phase 1 以双进程结果验收。RDMA 比较只在硬件存在时要求。

### 6.2 Phase 2

必须使用模拟 Worker 和合成 block，并通过 Phase 1 搬移字节。不得用延时桩代替传输。

对照策略：

- Round Robin
- 最短队列
- 只使用 \(L_i\)，即 [router.md](router.md) 的策略
- 同时使用 \(L_i\) 和 \(F_{si}\)，即 5.4

场景至少覆盖不同命中率、热点 prefix、不同远端占比、网络争用、负载不均，以及源节点在传输中失败。

指标是端到端完成时间的 P50/P95/P99、吞吐、远程传输字节、排队时间、传输等待和重算开销。

Phase 2 完成时，Fetch 和 Recompute 都必须在 5.5 的状态机中实际执行过。至少一个场景要说明加入 \(F_{si}\) 之后，相对只使用 \(L_i\) 改变了完成时间或传输量。没有收益时必须如实报告，不得只给绝对值。
