# Phase 3：cuTile Rust GPU KV Staging

本规格在 [2part.md](2part.md) 的传输与执行协调之上，增加可选的 Worker GPU 数据准备层。Operator 指 GPU 算子，不是 Kubernetes Operator。

## 1. 目标与边界

使用 cuTile Rust 实现批量 KV Gather、布局转换、Pack 和 Unpack，评测算子融合能否降低完整 Fetch 的完成时间。不预设加速倍数。

- [router.md](router.md) 不参与 GPU Staging，不修改协议或评分。
- Phase 1/2 保持 CPU-only 可运行、独立验收；Phase 3 默认关闭。
- 算子、宿主控制和 Reference 使用 Rust，不使用 Triton、Python、PyTorch 或 PyO3。
- Transfer Engine 仍只接收 Host `MemoryRegion`，不接收 GPU 地址，不改变 TCP/RDMA 语义。
- GPU 扩展不修改 Block Directory、执行计划或正式评分公式。

### 1.1 范围

必需：合成的单层 Paged KV Tensor，FP16/BF16，固定大小不可变 block，批量间接寻址，两种打包布局，Pinned Host Memory，真实 D2H、TCP、H2D，以及异步完成与背压。

可选优化：Tile 调优、Chunk Pipeline、双缓冲和多 Stream 重叠。正确的串行链路必须先完成。

不包含：Attention、模型推理、真实 vLLM KV 导入导出、精度转换、压缩、GPUDirect RDMA、CUDA IPC、通用 GPU 分配器和 KV Cache Manager。

## 2. 数据定义

### 2.1 Shape 与 Stride

`N`、`R` 分别是源、目标物理 slot 数；`M` 是传输 block 数；`H` 是 KV head 数；`T` 是每块 token 数；`D` 是 head dimension。K/V 轴长度为 2，每元素占 2 字节。

首版只接受下表的行优先连续布局。Stride 单位是元素，不是字节；其他 stride、负 stride 和重叠视图均拒绝。

| 对象 | Shape | Stride |
|---|---|---|
| 源 Cache | `[N, 2, H, T, D]` | `[2HTD, HTD, TD, D, 1]` |
| 目标 Cache | `[R, 2, H, T, D]` | `[2HTD, HTD, TD, D, 1]` |
| Packed，布局 A | `[M, 2, H, T, D]` | `[2HTD, HTD, TD, D, 1]` |
| Packed，布局 B | `[M, 2, T, H, D]` | `[2THD, THD, HD, D, 1]` |
| 源、目标 Block Table | `[M]` | `[1]` |

每块包含 `2HTD` 个元素，占 `4HTD` 字节。整个 Packed Buffer 连续，块 `i` 的字节偏移是 `i × 4HTD`。

### 2.2 形式化变换

`S[i]`、`U[i]` 分别表示源、目标物理 slot。所有赋值均复制原始 16 位 bit pattern，不做浮点运算。

```text
Pack A: Packed[i,k,h,t,d] = Cache[S[i],k,h,t,d]
Pack B: Packed[i,k,t,h,d] = Cache[S[i],k,h,t,d]

Unpack A: Destination[U[i],k,h,t,d] = Packed[i,k,h,t,d]
Unpack B: Destination[U[i],k,h,t,d] = Packed[i,k,t,h,d]
```

对应的线性元素偏移：

```text
Cache:    ((((s × 2 + k) × H + h) × T + t) × D + d)
Packed A: ((((i × 2 + k) × H + h) × T + t) × D + d)
Packed B: ((((i × 2 + k) × T + t) × H + h) × D + d)
```

### 2.3 合法输入

- `N/R/H/T/D` 为正；`M = 0` 是无分配、无 Launch 的成功空操作。
- Block Table 使用连续 `i32`，源索引在 `[0,N)`，目标索引在 `[0,R)`。源允许重复，目标不得重复。
- 每次 Launch 的 GPU 数据位于同一设备；源、Packed、目标存储不得互相别名。不同 Worker 可使用不同 GPU。
- 表、metadata 和源 block 在操作期间不可变。目标 slot 必须独占预约，不能覆盖已发布 block，也不能被其他 Batch 同时写入。
- 大小与偏移使用受检的 64 位整数计算，同时检查宿主地址范围及固定版本 cuTile 的维度、索引和 stride 限制；溢出或超预算时在 Launch 前拒绝。
- Host 验证后的表才可上传；在途表禁止外部修改。非整 Tile 的尾部不得越界访问。

`BlockId` 是逻辑身份，`BlockSlotId` 是物理位置，二者不得混用。`KvLayout` 记录版本、dtype、Shape、Stride 和打包格式。打包格式仅是可逆的传输表示，不改变 `BlockId` 的 layout；原有模型、layout、dtype 和分片兼容性校验仍然必需。

## 3. 算子设计

### 3.1 Pack

基线按连续输出划分 Tile，使用逻辑 block 索引读取 `S`，再从源 slot 读取有效元素。布局 A 不重排；布局 B 在同一 Kernel 中按第 2 节映射直接写入最终位置，不生成完整中间 Tensor。

首版每个 Program 处理一个 block 的一个 payload Tile，沿 `D` 优先连续访问。大 block 拆成多个 Tile；之后比较跨 Head/Token 的多维 Tile 与每 Program 多 block 的映射。不能假设布局 B 的读写都能同时完全合并。

### 3.2 Unpack

首版使用目标 slot 到 Packed block 的逆向表：长度为 `R`，未使用项为 `-1`。按目标物理 slot 及其 payload Tile 划分互不重叠的可写区域，只写入表中有映射的 slot。布局 B 使用相反映射恢复 Cache。

这避免直接把任意 Scatter 地址当作安全的独占分区。逆向表构建、上传及扫描空 slot 的成本必须计入 Benchmark。只启动被选中 slot 的优化，需先验证对应的独占视图或底层接口。

### 3.3 实现约束

- 优先使用 cuTile 的 Tensor、Partition 和 Tile 接口。需要底层访问时，将 `unsafe` 隔离在 GPU 模块内，并说明边界、独占性、依赖与生命周期。
- 不强制使用浮点加载路径；可以用等宽整数存储或 bitcast 保留 FP16/BF16 的全部位模式，禁止数值转换。
- 比较 Tile 大小、维度映射、每 Program block 数及固定版本支持的编译选项，不直接套用 Triton 的调优参数。
- 所有候选配置先通过正确性测试，再计时。Autotuning 不是首版要求。

## 4. Runtime 集成

### 4.1 组件

新增独立的 `tile` crate，不放进 Router。GPU feature 命名为 `gpu`，默认关闭；默认构建不探测或链接 CUDA。

| 组件 | 职责 |
|---|---|
| `GpuStaging` | 校验 Batch，提交 Pack、Unpack、D2H、H2D |
| `DeviceBuffer` | 持有显存及其设备身份 |
| `HostBuffer` | 持有有界的 Pinned Host 分配，提供受控的传输访问权 |
| `StagingBatch` | 绑定 BlockId、slot、布局、buffer 和预约 |
| `GpuTicket` | 持有在途资源，追踪 CUDA Event，交付一次终态 |
| Worker 接口 | 协调源准备、目标恢复、验证与发布 |

语义操作为 `pack`、`unpack`、`toHost`、`toDevice` 和 `wait`。提交返回 Ticket，不代表设备工作完成；成功等待后才取得可供下一阶段使用的输出。Runtime 不暴露可随意解引用的 GPU 裸指针。

Host Buffer 注册到 Phase 1 时保留原分配的强所有权，不默认复制到 `Vec`。若 Phase 1 只能接收自有 buffer，先验证最小的所有者适配；若必须复制，记录次数、字节数和耗时，不宣称零复制。CUDA 页锁定与 Phase 1 的内存注册是不同操作。

### 4.2 技术验证

固定 cuTile commit、Rust、CUDA、driver 和 `tileiras` 版本。GPU 路径使用 Linux 和受支持的 NVIDIA GPU；按官方兼容矩阵验证，不要求开发机具有 GPU。

进入 Runtime 集成前，必须完成小型 Spike：

1. 批量间接读取、布局 B、逆向表 Unpack 和尾部访问在真实 GPU 上正确执行。
2. 验证 FP16/BF16 位模式、输出分区和所需底层接口；不能证明安全的访问不得上线。
3. 使用配套 CUDA Rust 宿主库完成 Pinned 分配、D2H/H2D、Stream/Event 与异步等待，验证能否直接注册 Host Buffer。
4. 验证 Drop、取消、超时与跨 Stream 使用时的资源存活；不能假定库的 Future 自动满足本规格。
5. JIT、内存分配和可能阻塞的 CUDA 调用放在专用执行线程，不阻塞 Tokio 核心线程；设备完成由 Event 驱动或有界轮询通知。

Spike 记录所用 API、最小示例、正确性结果和已知限制。接口无法满足要求时暂停该里程碑并记录缺口，不静默改用其他 Kernel 框架。

## 5. 异步状态机

```text
Validate → Pin / Reserve → Pack → D2H → TCP
         → Receive / Validate → H2D → Unpack → Validate
         → Publish → Dispatch Compute → Retire
```

GPU 阶段之间使用同 Stream 顺序或显式 Event 依赖；生产者初始化也必须包含在依赖中。Host 仅在 D2H Event 完成后提交网络传输，接收完整且验证后才提交 H2D。

发送端在 D2H 完成后按每块 Packed 字节生成 CRC32C；接收端检查 Batch 身份、布局、长度和校验和后才执行 H2D。Unpack 完成后，首版模拟 Worker 将目标 slot 回读到 Host，用独立 Rust Reference 逐位核对；额外拷贝、同步和验证成本计入端到端结果。

不变量：

- Launch 返回不是完成；网络完成也不是目标 GPU block 就绪。
- 源 Pin 至少覆盖原 Phase 2 要求的传输期限及全部相关 GPU 读取。预约、指针表、Device/Host Buffer 持续存活到最后一个使用者实际结束。
- 请求终态与资源回收分开。Drop Future、Timeout 或 Cancel 只停止后续提交，已提交操作必须安全排空，之后才可复用内存。
- Batch 只交付一次终态。失败数据不得发布；Recompute 不得与尚未结束的 Unpack 写同一 slot。
- 有界限制 Batch 数、Device Buffer 字节、Pinned Host 字节和待处理任务数。耗尽时等待或失败，不无界排队。

故障沿用 Phase 2：首次网络提交前失败可改选可行计划或 Recompute；提交后失败不再 Fetch，安全清理后最多一次 Recompute。目标 Worker 失败仍中止请求。CUDA context 不可恢复或无法确认操作结束时隔离资源、停止该 Worker，不强行释放并继续运行。

### 5.1 可选流水线

按完整 block 切 Chunk，首版不拆分单个 block。双缓冲允许不同 Chunk 的 Pack、拷贝和网络阶段重叠；每个 Chunk 仍遵守上述依赖。

每个 Chunk 独立满足第 2 节的连续布局，`M` 取该 Chunk 的块数；metadata 保留所属 Batch、全局顺序和 BlockId，不要求同时分配整个 Batch 的 Packed Buffer。

每端从两组 Device/Host Buffer 开始。源端复用等待网络不再读取；目标端复用等待 H2D、Unpack 及验证不再使用。整个 Batch 验证完才发布本次新 block；任一 Chunk 失败则本 Batch 不发布。更早已发布的副本不受影响。

必须用 Timeline 证明实际重叠；多 Stream 或 `async` 本身不构成重叠证据。

## 6. 测试与 Benchmark

### 6.1 正确性

CPU Reference 按第 2 节独立实现，使用 16 位原始数据逐位比较。覆盖两种布局、两种 dtype、Pack/Unpack 往返、重复源、非法或重复目标、空 Batch、尾部 Tile、无关 slot 不变，以及零、无穷、NaN payload 和随机位模式。

GPU 集成测试覆盖各阶段取消、超时、网络断连、校验失败、资源耗尽、事件乱序通知和在途释放。通过真实操作配合受控故障验证；Mock 仅可用于 CPU 状态机测试，不代替 GPU 验收。

### 6.2 三层评测

| 层次 | 测量范围 | 对照 |
|---|---|---|
| 算子 | Pack、布局转换、Unpack | 连续 memcpy、逐块及批量 memcpy、分步 Gather + 重排、融合算子 |
| Staging | Pack → D2H → H2D → Unpack | 相同 GPU 数据与布局下的非融合、融合、串行及流水线 |
| Runtime | 源准备到目标发布，以及模拟请求完成 | 原 CPU Runtime、GPU 非融合、GPU 融合、GPU 流水线，使用真实 TCP |

Memcpy 仅在语义等价时作完整对照；布局 B 不能与未做重排的复制直接比较。融合与非融合都必须真实物化相同输出，不能用仅改变 Shape 的视图替代布局转换。

算子计时预分配 buffer、预热并排除首次 JIT；JIT 耗时另报。GPU 阶段用 CUDA Event，完整链路用 Host 单调时钟。表构建、上传、调度、验证和额外复制单独计量且纳入端到端时间。多机不用不同主机的未同步时钟直接相减。

必报指标：P50/P95/P99、吞吐、Pack/Unpack、D2H/H2D、网络与排队耗时、CPU 提交开销、验证成本、峰值 Device/Pinned Host 内存、Fetch 成功率、Recompute 次数与恢复成本。

算子有效带宽按 `读取 payload + 写入 payload` 除以时间计算；单次 Pack 或 Unpack 为两倍 payload 字节。索引流量另列，不把逻辑带宽称为实测 HBM 带宽。串行 Fetch 成本为 Pack、D2H、网络、H2D、Unpack、验证及协调开销之和；流水线用依赖 Timeline 和实测完成时间，不简单相加。

### 6.3 实验变量与记录

| 变量 | 建议取值 |
|---|---|
| T / H / D | `16,32,64` / `8,16,32` / `64,128` |
| M | `1,4,16,64,256,1024` |
| dtype / 布局 | FP16、BF16 / A、B |
| 物理分布 | 连续、随机、局部连续且整体分散 |
| 执行方式 | 分步、融合 / 串行、双缓冲 |

另测非整 Tile 形状，如 `H=3,T=17,D=80`。记录 `N/R`、逆向表成本和随机种子；显存预算不足的组合显式标记未运行及原因，不自动缩小后冒充原组合。

结果导出原始逐次 JSONL 和汇总 CSV，包含：commit、工具链、GPU、PCIe/网络拓扑、完整 Shape、布局、访问分布、算法、Tile 配置、Chunk 大小、预热与样本数、计时范围、各阶段耗时、内存峰值、正确性和失败状态。每个配置至少三轮独立稳态测量，默认每轮 1000 个完成样本；更少样本需说明，尤其不得据此夸大 P99 结论。

CPU 与 GPU 的内存位置不同，两者差异不能全部归因于融合。正式评分公式及原网络指标的采样范围保持不变，单独报告新增 Staging 成本与预测误差。模拟结果不描述为真实模型 TTFT、TPOT 或推理 GPU 利用率；无收益或退化必须保留。

## 7. 实施与完成定义

| 里程碑 | 交付与验收 |
|---|---|
| 1. 算子原型 | 环境 Spike、合成数据、Rust Reference、布局 A Pack/Unpack、逐位测试和基础计时 |
| 2. 融合优化 | 布局 B、Tile 映射比较、公平基线和原始性能结果；正确性通过后才调优 |
| 3. Runtime 集成 | 依赖 Phase 1/2 接口就绪，完成资源 Spike、两个独立 Worker 进程间的真实 D2H/TCP/H2D/Unpack、验证发布与取消测试 |
| 4. 可选流水线 | 有界 Chunk 双缓冲、Stream/Event 依赖、失败测试、重叠 Timeline 和内存成本 |
| 5. 系统评测 | 三层 Benchmark、阶段耗时分解、恢复成本，以及收益和退化场景报告 |

两个 Worker 可在一台 GPU 机器上以独立进程验收，必须明确共享硬件和 loopback 网络；有多机资源时复用同一套测试验证跨节点路径。

Phase 3 完成要求：必需算子在真实 NVIDIA GPU 上执行；逐位正确性、真实跨 Worker 传输、验证后发布和在途资源安全均通过；Benchmark 可复现且性能归因明确；原 CPU-only 测试不受影响。里程碑 4 不阻塞基础完成，但未完成不得声称支持流水线。

没有 GPU 时，GPU 测试明确报告未运行，不能视为 Phase 3 验收成功。当前仓库的 Phase 1/2 仍以规格为准，不能假定已有 Runtime 实现；算子里程碑可先独立开展。

## 8. 技术依据

- [cuTile Rust](https://github.com/NVIDIA/cuda-rust/tree/main/cutile-rs)：Tile Kernel、宿主 Runtime 与项目状态。
- [兼容矩阵](https://nvidia.github.io/cuda-rust/cutile/latest/reference/compatibility.html)：GPU、Tile IR 与工具链要求。
- [CUDA 异步语义](https://docs.nvidia.com/cuda/cuda-runtime-api/api-sync-behavior.html)：拷贝与 Kernel 提交不等于完成。
