# mini-router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust Endpoint Picker that renders an OpenAI chat request once, routes only among Gateway-provided candidates using vLLM KV events plus load, and forwards the same `token_ids` through a thin Worker adapter.

**Architecture:** Envoy AI Gateway owns the public OpenAI endpoint and Kubernetes membership. The Router implements only the EPP/ext-proc control protocol, maintains per-replica in-memory endpoint/KV/load state, and uses a local CPU-only vLLM Renderer. A Rust adapter in each Worker Pod translates the internal envelope into the pinned vLLM token-in field and streams the response without parsing it.

**Tech Stack:** Rust 1.94 / edition 2024, Tokio 1.53, Tonic 0.14, `envoy-grpc-ext-proc` 0.1.2, Axum 0.8, Reqwest 0.13, Serde, MessagePack, libzmq, Prometheus, Envoy AI Gateway 0.6, Gateway API Inference Extension 1.5, kind 0.32.

## Global Constraints

- Deliver the complete scope once; the numbered tasks below are implementation order, not product phases.
- Support one fixed model per deployment and text-only `/v1/chat/completions` requests.
- Reject multimodal input, multiple models, LoRA routing, tool/reasoning-specific behavior, and disaggregated serving.
- The Router exposes EPP/ext-proc and health/metrics only; it never exposes a public OpenAI endpoint or proxies generated response bodies.
- The Router never imports a Kubernetes SDK, watches Kubernetes resources, or implements registration, Lease, coordination, or state replication.
- Production candidate membership comes only from `envoy.lb.subset_hint/x-gateway-destination-endpoint-subset` in each EPP request.
- A routing decision can select only an endpoint in that request's `CandidateSet`.
- Render exactly once on the normal path; the same internal `token_ids` drive prefix matching and vLLM inference.
- Pin vLLM to commit `9c22668436a4d94aab87ea74a220e060415cf1d8`; the adapter is the only module that knows `kv_transfer_params.prompt_token_ids`.
- Use EPP protocol v1.0 semantics: write the same endpoint into the HTTP header and `envoy.lb` dynamic metadata, consume served-endpoint response metadata, and implement standard gRPC health.
- Configure ext-proc request bodies as buffered and response bodies as disabled.
- Every project-owned source, test, script, manifest, and container file has a one-word filename; do not introduce `_` or `-` in a filename stem.
- Commit `Cargo.lock`; deny warnings in CI; forbid unsafe code in project sources.
- Ordinary CI requires no GPU. Cross-component tests run in kind; real GPU Kubernetes verification is a release gate.

## Upstream Contracts

- EPP protocol: `envoy.lb.subset_hint` request metadata, `envoy.lb` response metadata, identical `x-gateway-destination-endpoint` header/metadata values, and `x-gateway-destination-endpoint-served` response metadata.
- vLLM Render: `POST /v1/chat/completions/render` returns a JSON object whose `token_ids` is a non-empty integer array.
- vLLM token-in: the adapter writes the rendered IDs to `kv_transfer_params.prompt_token_ids` on the pinned vLLM commit.
- vLLM KV PUB frames: `[topic, sequence_be_u64, msgpack(EventBatch)]`; replay requests send `sequence_be_u64` and end with sequence `-1`.
- `EventBatch` is MessagePack array-like: `[ts, events, data_parallel_rank?]`. `BlockStored` includes block hashes, parent hash, concatenated token IDs, and block size.

## File Map

```text
Cargo.toml
Cargo.lock
.gitignore
README.md
src/
  lib.rs
  config.rs
  error.rs
  types.rs
  telemetry.rs
  bin/
    router.rs
    adapter.rs
    mock.rs
  epp/
    server.rs
    candidates.rs
    response.rs
    health.rs
  renderer/
    client.rs
    request.rs
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
tests/
  config.rs
  candidates.rs
  renderer.rs
  events.rs
  index.rs
  manager.rs
  scorer.rs
  epp.rs
  adapter.rs
  fixtures/
    events/
      store.msgpack
      remove.msgpack
    renders/
      chat.json
  kind/
    cluster.yaml
    namespace.yaml
    gateway.yaml
    router.yaml
    worker.yaml
    pool.yaml
    route.yaml
    setup.sh
    verify.sh
    teardown.sh
  gpu/
    worker.yaml
    verify.sh
    bench.sh
deploy/
  k8s/
    router.yaml
    service.yaml
    pool.yaml
    route.yaml
    worker.yaml
docker/
  router/Dockerfile
  adapter/Dockerfile
  mock/Dockerfile
benches/
  router.rs
.github/
  workflows/
    ci.yaml
docs/
  specs/
    router.md
  superpowers/
    plans/
      router.md
```

`src/bin/mock.rs`, `docker/mock/Dockerfile`, and all `tests/` assets are test-only and are excluded from production images.

---

### Task 1: Bootstrap the crate and domain types

**Files:**
- Create: `Cargo.toml`
- Create: `.gitignore`
- Create: `src/lib.rs`
- Create: `src/error.rs`
- Create: `src/types.rs`
- Create: `src/config.rs`
- Test: `tests/config.rs`

**Interfaces:**
- Produces: `WorkerKey`, `CandidateSet`, `RenderedInput`, `RoutingRequest`, `RoutingDecision`, `RouterConfig`, and `AdapterConfig`.
- Consumes: no earlier project interfaces.

- [ ] **Step 1: Initialize Git and the Rust package**

Run:

```bash
git init
cargo init --name mini-router --lib .
```

Expected: `.git/`, `Cargo.toml`, and `src/lib.rs` exist. Replace Cargo's generated code in later steps; do not create `src/main.rs`.

- [ ] **Step 2: Pin the toolchain contract and dependencies**

Write this package section and direct dependencies into `Cargo.toml`; let Cargo resolve transitive versions and commit the resulting lockfile:

```toml
[package]
name = "mini-router"
version = "0.1.0"
edition = "2024"
rust-version = "1.94"

[dependencies]
anyhow = "1"
async-trait = "0.1"
axum = "0.8.9"
bytes = "1"
envoy-grpc-ext-proc = "0.1.2"
futures-util = "0.3"
http = "1"
prometheus = "0.14.0"
prometheus-parse = "0.2.5"
prost-types = "0.14"
reqwest = { version = "0.13.4", default-features = false, features = ["json", "rustls", "stream"] }
rmp-serde = "1.3.1"
serde = { version = "1.0.229", features = ["derive"] }
serde_bytes = "0.11"
serde_json = "1.0.151"
sha2 = "0.10"
subtle = "2"
thiserror = "2"
tokio = { version = "1.53.1", features = ["full"] }
tokio-stream = "0.1"
tokio-util = { version = "0.7", features = ["rt"] }
tonic = "0.14.6"
tonic-health = "0.14"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
uuid = { version = "1.24.0", features = ["v4"] }
zmq = "0.10"

[dev-dependencies]
criterion = { version = "0.7", features = ["async_tokio"] }
tower = "0.5"
wiremock = "0.6"

[[bin]]
name = "mini-router"
path = "src/bin/router.rs"

[[bin]]
name = "vllm-token-in-adapter"
path = "src/bin/adapter.rs"

[[bin]]
name = "mock"
path = "src/bin/mock.rs"
required-features = ["mock"]

[features]
mock = []

[[bench]]
name = "router"
harness = false
```

- [ ] **Step 3: Write failing domain/config tests**

Create `tests/config.rs`:

```rust
use mini_router::{config::RouterConfig, types::WorkerKey};

#[test]
fn endpoint_is_canonical() {
    let key: WorkerKey = "10.0.0.2:8200".parse().unwrap();
    assert_eq!(key.to_string(), "10.0.0.2:8200");
    assert_eq!(key.metrics(8000).to_string(), "10.0.0.2:8000");
}

#[test]
fn invalid_weights_are_rejected() {
    let mut config = RouterConfig::fixture();
    config.cache_weight = -1.0;
    assert!(config.validate().is_err());
}
```

- [ ] **Step 4: Run the tests and confirm the red state**

Run: `cargo test --test config`

Expected: FAIL because `RouterConfig` and `WorkerKey` do not exist.

- [ ] **Step 5: Implement the domain model and validated configuration**

Define these public shapes, using `SocketAddr` so candidates cannot silently contain DNS names:

```rust
pub struct WorkerKey(pub std::net::SocketAddr);
pub struct CandidateSet(pub std::collections::BTreeSet<WorkerKey>);
pub struct RenderedInput { pub token_ids: std::sync::Arc<[u32]> }
pub struct RendererIdentity {
    pub model: String,
    pub revision: String,
    pub probe_digest: String,
}
pub struct RoutingRequest {
    pub request_id: String,
    pub model: String,
    pub body: serde_json::Value,
    pub candidates: CandidateSet,
}
pub struct RoutingDecision {
    pub decision_id: uuid::Uuid,
    pub endpoint: WorkerKey,
    pub generation: u64,
    pub matched_tokens: usize,
    pub effective_load: f64,
    pub body: Vec<u8>,
}
```

`RouterConfig::validate()` must reject zero ports, zero limits/TTLs, negative weights, and an empty model/revision/probe digest. `AdapterConfig` must require the model ID, loopback vLLM URL, maximum body/token limits, and internal authentication token.

- [ ] **Step 6: Export modules, format, and pass the first gate**

Put `#![forbid(unsafe_code)]` and module exports in `src/lib.rs`.

Run:

```bash
cargo fmt --check
cargo test --test config
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock .gitignore src tests/config.rs
git commit -m "chore: bootstrap router crate"
```

---

### Task 2: Parse EPP candidates and build protocol responses

**Files:**
- Create: `src/epp/candidates.rs`
- Create: `src/epp/response.rs`
- Modify: `src/lib.rs`
- Test: `tests/candidates.rs`

**Interfaces:**
- Consumes: `WorkerKey`, `CandidateSet`, `RoutingDecision` from Task 1.
- Produces: `parse(metadata, limit) -> Result<CandidateSet, CandidateError>`, `picked(decision) -> ProcessingResponse`, `unavailable(message) -> ProcessingResponse`, and `served(metadata) -> Result<Option<WorkerKey>, CandidateError>`.

- [ ] **Step 1: Write metadata parsing tests**

Create `tests/candidates.rs` with a `prost_types::Struct` containing:

```rust
let subset = metadata(json!({
    "envoy.lb.subset_hint": {
        "x-gateway-destination-endpoint-subset": [
            "10.0.0.2:8200", "10.0.0.3:8200", "10.0.0.2:8200"
        ]
    }
}));
let parsed = parse(&subset, 8).unwrap();
assert_eq!(parsed.len(), 2);
```

Add separate assertions for a missing namespace, an empty list, a DNS name, port zero, and a list over the configured limit.

- [ ] **Step 2: Run the parser test and confirm failure**

Run: `cargo test --test candidates parse`

Expected: FAIL because the `epp` module does not exist.

- [ ] **Step 3: Implement strict candidate parsing**

Use these constants and signature:

```rust
pub const SUBSET_NAMESPACE: &str = "envoy.lb.subset_hint";
pub const SUBSET_KEY: &str = "x-gateway-destination-endpoint-subset";

pub fn parse(
    metadata: &prost_types::Struct,
    limit: usize,
) -> Result<CandidateSet, CandidateError>;
```

Accept only a protobuf list of string `<ip:port>` values, canonicalize through `SocketAddr`, deduplicate into `BTreeSet`, and reject missing/empty/oversized data. Do not read candidates from HTTP headers or application JSON.

- [ ] **Step 4: Write response-builder tests**

Assert that `picked()` produces both:

```text
x-gateway-destination-endpoint: 10.0.0.2:8200
dynamic_metadata["envoy.lb"]["x-gateway-destination-endpoint"] = "10.0.0.2:8200"
```

Assert the two strings are byte-for-byte identical, the body mutation replaces the buffered request body, and `unavailable()` is an `ImmediateResponse` with status 503. Add a served-metadata test for `envoy.lb/x-gateway-destination-endpoint-served`.

- [ ] **Step 5: Implement response construction**

Use:

```rust
pub const PICK_NAMESPACE: &str = "envoy.lb";
pub const PICK_KEY: &str = "x-gateway-destination-endpoint";
pub const SERVED_KEY: &str = "x-gateway-destination-endpoint-served";

pub fn picked(decision: &RoutingDecision) -> ProcessingResponse;
pub fn unavailable(message: &str) -> ProcessingResponse;
pub fn served(metadata: &prost_types::Struct) -> Result<Option<WorkerKey>, CandidateError>;
```

Build header mutation and dynamic metadata from one local `endpoint: String` variable so they cannot diverge.

- [ ] **Step 6: Verify and commit**

```bash
cargo fmt --check
cargo test --test candidates
cargo clippy --all-targets --all-features -- -D warnings
git add src/epp src/lib.rs tests/candidates.rs
git commit -m "feat: add endpoint picker metadata"
```

---

### Task 3: Validate chat requests and call the vLLM Renderer

**Files:**
- Create: `src/renderer/request.rs`
- Create: `src/renderer/client.rs`
- Modify: `src/lib.rs`
- Test: `tests/renderer.rs`
- Create: `tests/fixtures/renders/chat.json`

**Interfaces:**
- Consumes: `RenderedInput`, `RouterConfig` from Task 1.
- Produces: `validate(body, config) -> Result<Value, RequestError>` and async trait `Renderer::render(&Value) -> Result<RenderedInput, RenderError>`.

- [ ] **Step 1: Write request-validation tests**

Cover one valid request and these exact rejections:

```json
{"model":"other","messages":[{"role":"user","content":"hi"}]}
{"model":"fixed","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}
{"model":"fixed","messages":[],"mini_router":{"token_ids":[1]}}
```

The first rejects the model, the second rejects non-string content, and the third removes the reserved field before rendering while still rejecting empty messages.

- [ ] **Step 2: Write the failing Renderer contract test**

Use WireMock to return `tests/fixtures/renders/chat.json`:

```json
{"request_id":"render-1","token_ids":[1,2,3,4],"sampling_params":{}}
```

Assert that `HttpRenderer::render()` returns exactly `[1,2,3,4]`, rejects an empty `token_ids`, and sends neither `mini_router` nor client-provided `kv_transfer_params`. Add an identity-probe response and assert readiness fails on a wrong model root or wrong SHA-256 token digest.

- [ ] **Step 3: Run the red tests**

Run: `cargo test --test renderer`

Expected: FAIL because validation and renderer client types are missing.

- [ ] **Step 4: Implement validation and sanitization**

Deserialize only the fields needed for validation while retaining the original `serde_json::Value`:

```rust
#[derive(Deserialize)]
struct Chat<'a> {
    model: &'a str,
    messages: Vec<Message<'a>>,
}

#[derive(Deserialize)]
struct Message<'a> {
    role: &'a str,
    content: &'a str,
}
```

Reject bodies over `max_body_bytes`; require at least one message; accept only `system`, `user`, `assistant`, and `developer` roles; remove `mini_router` and `kv_transfer_params` from the sanitized clone.

- [ ] **Step 5: Implement the Renderer client**

Define:

```rust
#[async_trait::async_trait]
pub trait Renderer: Send + Sync {
    async fn render(&self, body: &serde_json::Value) -> Result<RenderedInput, RenderError>;
    async fn ready(&self, model: &str) -> Result<(), RenderError>;
}
```

`HttpRenderer` must reuse one `reqwest::Client`, POST to `/v1/chat/completions/render`, enforce `renderer_timeout`, deserialize only `token_ids`, and reject zero IDs or more than `max_token_count`.

In `ready()`, check `/v1/models` for the configured served model/root, render one fixed deployment probe request, hash the returned IDs as canonical big-endian `u32` bytes with SHA-256, and compare it to `RendererIdentity::probe_digest`. This single digest verifies the deployed tokenizer revision, chat template, and special-token behavior even though stock vLLM exposes no separate identity endpoint. Generate the expected digest once from the pinned model revision and use the same immutable identity in Renderer and Worker manifests.

- [ ] **Step 6: Verify and commit**

```bash
cargo fmt --check
cargo test --test renderer
cargo clippy --all-targets --all-features -- -D warnings
git add src/renderer src/lib.rs tests/renderer.rs tests/fixtures/renders/chat.json
git commit -m "feat: add vllm renderer client"
```

---

### Task 4: Decode vLLM KV events and implement replay-aware subscription

**Files:**
- Create: `src/kv/events.rs`
- Create: `src/kv/subscriber.rs`
- Modify: `src/lib.rs`
- Test: `tests/events.rs`
- Create: `tests/fixtures/events/store.msgpack`
- Create: `tests/fixtures/events/remove.msgpack`

**Interfaces:**
- Produces: `BlockHash`, `CacheEvent`, `EventBatch`, `SequencedBatch`, `decode(frames)`, and `Subscriber::run(endpoint, replay, sender, cancel)`.
- Consumes: `WorkerKey` and endpoint ports from Task 1.

- [ ] **Step 1: Capture deterministic fixtures from the pinned vLLM schema**

At vLLM commit `9c22668436a4d94aab87ea74a220e060415cf1d8`, run this against the local pinned checkout:

```bash
uv run --project /Users/allegre7tto/Coding/Learn/Infra/vllm python -c 'from pathlib import Path; import msgspec; from vllm.distributed.kv_events import KVEventBatch,BlockStored,BlockRemoved; out=Path("/Users/allegre7tto/Coding/mini-router/tests/fixtures/events"); out.mkdir(parents=True,exist_ok=True); store=KVEventBatch(ts=1.0,events=[BlockStored(block_hashes=[b"a",b"b"],parent_block_hash=None,token_ids=[1,2,3,4,5,6,7,8],block_size=4,lora_id=None,medium="GPU",lora_name=None)]); remove=KVEventBatch(ts=2.0,events=[BlockRemoved(block_hashes=[b"b"],medium="GPU")]); (out/"store.msgpack").write_bytes(msgspec.msgpack.encode(store)); (out/"remove.msgpack").write_bytes(msgspec.msgpack.encode(remove))'
```

Expected: the two payload files decode through the pinned Python types. Sequence and topic frames are constructed in Rust tests.

- [ ] **Step 2: Write failing decoder tests**

Test this public representation:

```rust
pub enum BlockHash { Bytes(Vec<u8>), Integer(u64) }
pub enum CacheEvent {
    Stored { hashes: Vec<BlockHash>, parent: Option<BlockHash>, tokens: Vec<u32>, size: usize, group: u32 },
    Removed { hashes: Vec<BlockHash>, group: u32 },
    Cleared,
}
pub struct SequencedBatch { pub sequence: u64, pub batch: EventBatch }
```

Assert `decode([b"kv-events", seq_be, payload])` preserves sequence, hashes, token order, block size, and default `group_idx = 0`. Reject a frame count other than three, negative tokens, or `tokens.len() != hashes.len() * block_size`.

- [ ] **Step 3: Run the decoder tests and confirm failure**

Run: `cargo test --test events decode`

Expected: FAIL because `kv::events` is missing.

- [ ] **Step 4: Implement MessagePack decoding**

Mirror msgspec's array-like batch and tagged event maps with private Serde structs. Use `serde_bytes::ByteBuf` for byte hashes and an untagged enum for byte-or-integer hashes. Convert the wire structs into the public validated representation before returning.

- [ ] **Step 5: Write subscriber/replay tests**

Start local libzmq PUB and ROUTER sockets on random ports. Publish sequences `0,1,3`, serve sequence `2` from replay, and assert the subscriber emits `0,1,2,3` once. Add a case where replay begins after the requested sequence and assert it emits `SubscriberError::Gap { expected, available }`.

- [ ] **Step 6: Implement the blocking ZMQ loop behind a Tokio channel**

Use one `spawn_blocking` task per observed endpoint. The loop must:

```text
SUB connect -> subscribe topic -> receive multipart
sequence == expected -> send batch -> expected += 1
sequence < expected  -> ignore duplicate
sequence > expected  -> REQ replay(expected) -> emit ordered replay -> resume live
cancelled            -> close sockets with linger 0
```

Set receive timeouts so cancellation is observed within one second. Never block a Tokio worker thread on libzmq calls.

- [ ] **Step 7: Verify and commit**

```bash
cargo fmt --check
cargo test --test events
cargo clippy --all-targets --all-features -- -D warnings
git add src/kv src/lib.rs tests/events.rs tests/fixtures/events
git commit -m "feat: consume vllm cache events"
```

---

### Task 5: Build the generation-safe prefix index

**Files:**
- Create: `src/kv/index.rs`
- Modify: `src/lib.rs`
- Test: `tests/index.rs`

**Interfaces:**
- Consumes: validated `CacheEvent`, `EventBatch`, `WorkerKey`, and endpoint generation.
- Produces: `PrefixIndex::apply`, `PrefixIndex::clear`, and `PrefixIndex::match_prefix`.

- [ ] **Step 1: Write the block-chain tests**

Build two endpoints and these chains in `tests/index.rs`:

```text
worker A: [1,2,3,4] -> [5,6,7,8]
worker B: [1,2,3,4] -> [9,10,11,12]
query:    [1,2,3,4,5,6,7,8,99]
```

Assert A matches eight tokens, B matches four, a candidate with no ownership matches zero, and a non-candidate is absent from the result. Add tests for duplicate stores, removal of one owner from a shared block, `AllBlocksCleared`, and rejection of a child whose declared parent is unknown.

- [ ] **Step 2: Write generation-isolation tests**

Store a chain for `(worker A, generation 1)`, clear generation 1, then store a different chain for generation 2. Assert that delayed generation-1 events cannot recreate ownership and generation-1 hashes never contribute to generation-2 matches.

- [ ] **Step 3: Run the red tests**

Run: `cargo test --test index`

Expected: FAIL because `PrefixIndex` is missing.

- [ ] **Step 4: Implement ownership and reverse indexes**

Use these project-owned types:

```rust
pub struct Owner {
    pub worker: WorkerKey,
    pub generation: u64,
}

pub struct PrefixMatch {
    pub blocks: usize,
    pub tokens: usize,
}

impl PrefixIndex {
    pub fn apply(
        &mut self,
        worker: WorkerKey,
        generation: u64,
        batch: &EventBatch,
    ) -> Result<(), IndexError>;

    pub fn clear(&mut self, worker: WorkerKey, generation: u64);

    pub fn match_prefix(
        &self,
        token_ids: &[u32],
        candidates: &CandidateSet,
        generations: &BTreeMap<WorkerKey, u64>,
    ) -> BTreeMap<WorkerKey, PrefixMatch>;
}
```

Keep one node per `(group, block_hash)` with its parent hash, exact token block, and owner set. Keep a reverse `(worker, generation) -> block keys` index so clear and endpoint expiry do not scan the whole graph. For a stored batch, split concatenated tokens by `block_size`, attach the first block to the event parent and each later block to the preceding hash, then add ownership idempotently.

- [ ] **Step 5: Implement continuous-prefix lookup**

Walk only complete stored blocks, beginning at the root and following exact token-block edges. Count a block for a worker only when the same `(worker, current_generation)` owns every block in the traversed chain. Stop at the first missing block; never add disjoint later blocks.

For this first implementation, require the fixed model to emit one KV cache group (`group_idx = 0`) and `data_parallel_rank` absent or zero. Mark any endpoint that violates that contract untrusted instead of reporting an inflated hit.

- [ ] **Step 6: Verify and commit**

```bash
cargo fmt --check
cargo test --test index
cargo clippy --all-targets --all-features -- -D warnings
git add src/kv src/lib.rs tests/index.rs
git commit -m "feat: add prefix ownership index"
```

---

### Task 6: Track endpoint load, trust, lifecycle, and reservations

**Files:**
- Create: `src/workers/endpoint.rs`
- Create: `src/workers/manager.rs`
- Create: `src/routing/load.rs`
- Modify: `src/lib.rs`
- Test: `tests/manager.rs`

**Interfaces:**
- Consumes: `CandidateSet`, subscriber batches/errors, vLLM Prometheus text, and `PrefixIndex`.
- Produces: immutable `EndpointSnapshot` values and reservation lifecycle operations.

- [ ] **Step 1: Write Prometheus load parsing tests**

Use an in-memory metrics payload containing:

```text
vllm:num_requests_running{model_name="fixed"} 2
vllm:num_requests_waiting{model_name="fixed"} 3
vllm:kv_cache_usage_perc{model_name="fixed"} 0.5
```

Assert the parser returns `running = 2`, `waiting = 3`, and `kv_usage = 0.5`. Assert duplicate samples are summed only across replicas explicitly belonging to this endpoint, non-finite/negative values fail, and a sample older than `metrics_max_age` is unavailable to the scorer.

- [ ] **Step 2: Write endpoint lifecycle tests with paused Tokio time**

Cover all transitions:

```text
first observed candidate -> generation 1, warming, background tasks started once
valid replay/live event  -> trusted
recoverable gap          -> temporarily untrusted -> trusted after replay
unrecoverable gap        -> generation increments, ownership clears, rebuilding
candidate absent         -> immediately ineligible but state retained
absent beyond TTL        -> background tasks cancelled and state removed
```

Also assert a stale background task cannot publish events after its generation is cancelled.

- [ ] **Step 3: Write reservation tests**

Reserve two decisions on worker A and assert `effective_load = running + waiting + 2`. Complete one using served-endpoint metadata and let the other expire under paused time. Assert duplicate/unknown completions are harmless and a served endpoint different from the reserved endpoint increments a mismatch result without decrementing the wrong worker.

- [ ] **Step 4: Run the red tests**

Run: `cargo test --test manager`

Expected: FAIL because endpoint management is missing.

- [ ] **Step 5: Implement the immutable snapshot model**

```rust
pub enum Trust { Warming, Trusted, Untrusted }

pub struct LoadSample {
    pub running: u64,
    pub waiting: u64,
    pub kv_usage: f64,
    pub collected_at: std::time::Instant,
}

pub struct EndpointSnapshot {
    pub key: WorkerKey,
    pub generation: u64,
    pub trust: Trust,
    pub load: Option<LoadSample>,
    pub reservations: usize,
}
```

Parse only the three configured vLLM metric names. Fetch metrics periodically through one reused Reqwest client. Do not derive Kubernetes identity, Pod names, or readiness from metrics labels.

- [ ] **Step 6: Implement `EndpointManager`**

```rust
impl EndpointManager {
    pub async fn observe(&self, candidates: &CandidateSet, now: Instant);
    pub async fn snapshots(&self, candidates: &CandidateSet) -> Vec<EndpointSnapshot>;
    pub async fn reserve(&self, decision: Uuid, worker: WorkerKey, expires: Instant);
    pub async fn complete(&self, decision: Uuid, served: WorkerKey) -> Completion;
    pub async fn reap(&self, now: Instant);
}
```

The first observation lazily starts exactly one metrics poller and one KV subscriber for that socket address. All task output carries endpoint generation; the manager discards output from older generations. On unrecoverable replay, clear the old generation in `PrefixIndex`, increment generation, and remain untrusted until a contiguous new event arrives. On endpoint expiry, cancel tasks, clear ownership, load, and reservations.

- [ ] **Step 7: Verify and commit**

```bash
cargo fmt --check
cargo test --test manager
cargo clippy --all-targets --all-features -- -D warnings
git add src/workers src/routing/load.rs src/lib.rs tests/manager.rs
git commit -m "feat: manage endpoint state"
```

---

### Task 7: Implement deterministic KV-and-load routing

**Files:**
- Create: `src/routing/scorer.rs`
- Create: `src/routing/service.rs`
- Modify: `src/lib.rs`
- Test: `tests/scorer.rs`

**Interfaces:**
- Consumes: validated request, one `RenderedInput`, prefix matches, endpoint snapshots, and `RouterConfig`.
- Produces: one `RoutingDecision` and one short-lived reservation.

- [ ] **Step 1: Write pure scoring tests**

Use the exact formula:

```text
cache_score = matched_prefix_tokens / token_count
load_score = (running + waiting + local_reservations) / max_concurrency
final_score = cache_weight * cache_score - load_weight * load_score
```

Test that a cache-rich worker wins when its bonus exceeds load cost, a heavily loaded worker loses, an untrusted worker receives zero cache bonus, and stale/missing metrics trigger the deterministic fallback. For every case, assert the selected endpoint belongs to the input `CandidateSet`.

- [ ] **Step 2: Define and test deterministic ties**

For equal scores, sort candidates and select the lexicographically smallest SHA-256 digest of `request_id || 0x00 || canonical_endpoint`. Compare scores with `f64::total_cmp`, then digest bytes, then canonical endpoint as the final tie-breaker. Run the same test 100 times and assert the same result; change the request ID and assert selection may distribute across candidates without randomness or process-local seed state.

- [ ] **Step 3: Write a routing-service test with mocks**

Provide a `Renderer` mock that panics on a second call, an in-memory prefix index, and two endpoint snapshots. Assert `RoutingService::route()`:

1. validates/sanitizes the body;
2. observes only request candidates;
3. calls Renderer exactly once;
4. queries the index with those exact IDs;
5. adds a version-1 `mini_router` envelope to the sanitized original body;
6. reserves the chosen endpoint;
7. returns endpoint, generation, matched tokens, effective load, and mutated body.

- [ ] **Step 4: Run the red tests**

Run: `cargo test --test scorer`

Expected: FAIL because scorer/service types are missing.

- [ ] **Step 5: Implement the pure scorer**

```rust
pub fn choose(
    request_id: &str,
    token_count: usize,
    candidates: &CandidateSet,
    matches: &BTreeMap<WorkerKey, PrefixMatch>,
    snapshots: &[EndpointSnapshot],
    config: &RouterConfig,
) -> Result<ScoredEndpoint, RoutingError>;
```

If at least one candidate has fresh metrics, use the formula and assign candidates without fresh metrics the worst known load plus one slot. If none has fresh metrics, ignore load and choose by trusted cache score, then deterministic hash. Never use `kv_usage` as a fourth undocumented scoring term; export it only as telemetry.

- [ ] **Step 6: Implement the orchestration service**

Construct the body mutation from the sanitized original body and this reserved envelope:

```json
{
  "mini_router": {
    "version": 1,
    "decision_id": "<uuid>",
    "token_ids": [1, 2, 3]
  }
}
```

Create the reservation only after serialization succeeds. If reservation creation fails, return no decision. Do not log request content or token values.

- [ ] **Step 7: Verify and commit**

```bash
cargo fmt --check
cargo test --test scorer
cargo clippy --all-targets --all-features -- -D warnings
git add src/routing src/lib.rs tests/scorer.rs
git commit -m "feat: route by cache and load"
```

---

### Task 8: Serve the EPP/ext-proc stream and gRPC health

**Files:**
- Create: `src/epp/server.rs`
- Create: `src/epp/health.rs`
- Modify: `src/lib.rs`
- Test: `tests/epp.rs`

**Interfaces:**
- Consumes: EPP protobuf stream messages and `RoutingService`.
- Produces: protocol-compliant `ExternalProcessor` responses and standard gRPC health status.

- [ ] **Step 1: Write an in-process gRPC protocol test**

Start Tonic on an ephemeral port and drive this stream:

```text
request_headers(metadata with two candidates) -> Continue
request_body(end_of_stream=true)               -> body replacement + endpoint header + metadata
response_headers(served endpoint metadata)     -> Continue and release reservation
end stream
```

Assert Renderer is called once, endpoint header equals dynamic metadata, the body contains `token_ids`, and the service never requests or consumes a response body.

- [ ] **Step 2: Add failure and limit tests**

Assert malformed JSON returns 400, body over the configured limit returns 413, missing/empty candidates returns 503, Renderer failure returns 503, and a request-body message received before request headers terminates the stream with `invalid_argument`. Assert an absent served endpoint leaves the reservation for TTL expiry.

- [ ] **Step 3: Add health tests**

Query `grpc.health.v1.Health/Check` for `""` and the external processor service name. Liveness is serving once the process event loop is running. Readiness is serving only when config is valid, the local Renderer identity check passes, and routing state can accept requests; it must not depend on Kubernetes APIs or require all possible Workers to be warm.

- [ ] **Step 4: Run the red tests**

Run: `cargo test --test epp`

Expected: FAIL because the gRPC server is missing.

- [ ] **Step 5: Implement one state machine per stream**

Buffer request-body chunks up to `max_body_bytes`. Parse candidates only from request-stage metadata. Accept an ASCII `x-request-id` up to 128 bytes from request headers; generate a UUID when it is absent or invalid. Route only on the chunk with `end_of_stream = true`. Return `ProcessingResponse` messages in the same stage order Envoy expects. At response headers, parse only `envoy.lb/x-gateway-destination-endpoint-served`, complete the current stream's decision if it matches a known reservation, and immediately return Continue.

Reject any `response_body` message as a deployment misconfiguration. The Router participates in the ext-proc control stream but never reads, buffers, transforms, or proxies inference response bytes.

- [ ] **Step 6: Register both services on one Tonic server**

Register the generated `ExternalProcessorServer` and `tonic_health::server::health_reporter()` on the configured internal address. Update health status when Renderer readiness changes. Do not add HTTP OpenAI routes.

- [ ] **Step 7: Verify and commit**

```bash
cargo fmt --check
cargo test --test epp
cargo clippy --all-targets --all-features -- -D warnings
git add src/epp src/lib.rs tests/epp.rs
git commit -m "feat: serve endpoint picker protocol"
```

---

### Task 9: Implement the thin vLLM token-in adapter

**Files:**
- Create: `src/adapter/transform.rs`
- Create: `src/adapter/proxy.rs`
- Modify: `src/lib.rs`
- Test: `tests/adapter.rs`

**Interfaces:**
- Consumes: a Gateway-authenticated chat request with or without `mini_router`.
- Produces: a sanitized request to the Pod-local vLLM server and a byte-streaming response.

- [ ] **Step 1: Write normal-path transformation tests**

Given an envelope with `token_ids = [1,2,3]`, assert the transformed body:

- removes `mini_router`;
- preserves `model`, `messages`, sampling fields, and `stream`;
- overwrites the entire client-provided `kv_transfer_params` object;
- contains exactly `{"kv_transfer_params":{"prompt_token_ids":[1,2,3]}}`;
- rejects wrong envelope versions, wrong model, empty IDs, over-limit IDs, duplicate envelope fields, and non-integer/negative IDs.

- [ ] **Step 2: Write raw-fallback security tests**

Require `subtle::ConstantTimeEq` comparison of an internal bearer token from a configured header. An authenticated request without `mini_router` forwards the raw prompt after deleting client-provided `kv_transfer_params`; an unauthenticated request returns 401. Apply authentication to both envelope and fallback paths so the adapter is never a public bypass.

- [ ] **Step 3: Write proxy streaming tests**

Use WireMock as loopback vLLM. Return three delayed SSE chunks and assert the adapter exposes the first chunk before the upstream finishes, preserves status and content type, enforces backpressure, and never buffers the full body. Assert hop-by-hop headers and the internal auth header are not forwarded.

- [ ] **Step 4: Run the red tests**

Run: `cargo test --test adapter`

Expected: FAIL because adapter modules are missing.

- [ ] **Step 5: Implement strict envelope transformation**

First pass the raw JSON through a small Serde map visitor that rejects duplicate top-level `mini_router` or `kv_transfer_params` keys; `serde_json::Value` alone is insufficient because it accepts last-key-wins duplicates. Then deserialize the envelope with `#[serde(deny_unknown_fields)]`, validate `version == 1`, configured model, decision UUID, and token count, and remove it before inserting the pinned private vLLM field in one place. Both paths must first remove any client-supplied `kv_transfer_params`; never merge untrusted nested keys.

- [ ] **Step 6: Implement the streaming reverse proxy**

Expose only `POST /v1/chat/completions`, `GET /health`, and `GET /metrics` on the adapter. Require the configured workload token, body limit, and JSON content type before transformation. Send to the fixed loopback vLLM URL with one Reqwest client, then build the Axum body from `response.bytes_stream()` without collecting it. Propagate cancellation by dropping the upstream response when the downstream disconnects.

- [ ] **Step 7: Verify and commit**

```bash
cargo fmt --check
cargo test --test adapter
cargo clippy --all-targets --all-features -- -D warnings
git add src/adapter src/lib.rs tests/adapter.rs
git commit -m "feat: add vllm token adapter"
```

---

### Task 10: Add telemetry and production binaries

**Files:**
- Create: `src/telemetry.rs`
- Create: `src/bin/router.rs`
- Create: `src/bin/adapter.rs`
- Modify: `src/lib.rs`
- Test: `tests/config.rs`

**Interfaces:**
- Consumes: validated environment/config files and all runtime services.
- Produces: the `mini-router` and `vllm-token-in-adapter` processes.

- [ ] **Step 1: Write metric registration and redaction tests**

Instantiate two registries to catch duplicate-registration bugs. Assert the Router exports request count/latency, token count, matched blocks/tokens, endpoint decisions, fallbacks, event duplicate/gap/replay counts, trust/generation/watermark, metrics age, index size, and reservation count. Assert the adapter exports request modes, invalid envelopes, upstream latency/errors, and active streams.

Capture JSON logs for a request containing a unique prompt and token sequence; assert neither value appears.

- [ ] **Step 2: Run the red tests**

Run: `cargo test telemetry --all-features`

Expected: FAIL because the metric registry and binaries are missing.

- [ ] **Step 3: Implement telemetry with bounded labels**

Use Prometheus histograms/counters/gauges and JSON tracing. Allow endpoint and reason labels; never use request ID, prompt, model input, decision ID, or token values as labels. Bind Router metrics on a separate internal HTTP port with only `/metrics` and `/health`.

- [ ] **Step 4: Implement Router process wiring**

The binary must:

1. load and validate config;
2. construct Renderer, PrefixIndex, EndpointManager, RoutingService, telemetry, EPP, and health;
3. fail readiness until Renderer identity succeeds;
4. run gRPC, metrics HTTP, reaper, and readiness refresh loops under one cancellation token;
5. stop accepting new streams on SIGTERM, cancel endpoint tasks, and finish shutdown within the configured grace period.

- [ ] **Step 5: Implement adapter process wiring**

Load config, bind the adapter HTTP server, verify the vLLM URL is loopback, expose health/metrics, and use graceful SIGTERM shutdown. Do not discover Workers or import Router modules that know scoring/KV state.

- [ ] **Step 6: Verify and commit**

```bash
cargo fmt --check
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo build --release --bins --all-features
git add src Cargo.toml Cargo.lock tests/config.rs
git commit -m "feat: wire runtime services"
```

---

### Task 11: Build a deterministic no-GPU mock Worker

**Files:**
- Create: `src/bin/mock.rs`
- Create: `docker/mock/Dockerfile`
- Test: `tests/events.rs`
- Test: `tests/adapter.rs`

**Interfaces:**
- Produces: test-only Render, chat, metrics, KV PUB/replay, and control endpoints.
- Consumes: no production-only interfaces.

- [ ] **Step 1: Write a black-box mock contract test**

Start `mock` as a child process on ephemeral ports and verify:

```text
POST /v1/chat/completions/render -> deterministic token_ids for fixture prompts
POST /v1/chat/completions        -> records prompt_token_ids and returns JSON or delayed SSE
GET  /metrics                    -> configured vLLM metric names
POST /control/load               -> updates running/waiting/kv usage
POST /control/store              -> publishes BlockStored and records replay history
POST /control/remove             -> publishes BlockRemoved
POST /control/clear              -> publishes AllBlocksCleared
POST /control/gap                -> skips a live sequence while optionally retaining replay
GET  /control/last               -> last transformed vLLM request, test-only
```

- [ ] **Step 2: Run the red test**

Run: `cargo test --features mock mock`

Expected: FAIL because the mock binary does not exist.

- [ ] **Step 3: Implement the mock with the real wire schemas**

Use the same JSON and MessagePack shapes accepted by production clients, but do not reuse scoring decisions or write directly into Router memory. Tokenization may be a deterministic whitespace-to-integer fixture algorithm because only the vLLM engine is mocked; all network boundaries remain real.

Keep a bounded sequence replay ring. The gap endpoint must support both recoverable and unrecoverable gaps so kind tests can exercise both branches.

- [ ] **Step 4: Add the mock container**

Build only the feature-gated `mock` binary in a multi-stage image and run as a numeric non-root user with a read-only root filesystem-compatible working directory.

- [ ] **Step 5: Verify and commit**

```bash
cargo fmt --check
cargo test --features mock mock
docker build -f docker/mock/Dockerfile -t mini-router-mock:test .
git add src/bin/mock.rs docker/mock tests
git commit -m "test: add mock vllm worker"
```

---

### Task 12: Package runtime containers and Kubernetes resources

**Files:**
- Create: `docker/router/Dockerfile`
- Create: `docker/adapter/Dockerfile`
- Create: `deploy/k8s/router.yaml`
- Create: `deploy/k8s/service.yaml`
- Create: `deploy/k8s/pool.yaml`
- Create: `deploy/k8s/route.yaml`
- Create: `deploy/k8s/worker.yaml`
- Create: `README.md`

**Interfaces:**
- Consumes: Router/adapter binaries and upstream Gateway/Inference Extension CRDs.
- Produces: reference production topology without a project-owned controller.

- [ ] **Step 1: Build minimal production images**

Use multi-stage builds pinned by digest. The Router image contains only the Rust binary. The pinned CPU-only vLLM Renderer runs as a second container in the Router Pod so each process has independent health and resource limits. The adapter image contains only the Rust adapter and CA roots; vLLM remains a separate loopback-bound container in the Worker Pod.

Run all containers as non-root, drop capabilities, use a read-only root filesystem, and expose only configured internal ports.

- [ ] **Step 2: Write the Router deployment and Service**

Create two Router replicas, readiness/liveness probes, CPU/memory requests, topology spread, Pod disruption budget, ConfigMap, Secret reference for internal workload authentication, and ClusterIP Services for gRPC and metrics. Give the Router ServiceAccount no Role or RoleBinding; this is an executable proof that it performs no Kubernetes discovery.

- [ ] **Step 3: Write InferencePool and route resources**

Pin manifests to Gateway API Inference Extension 1.5 and Envoy AI Gateway 0.6 APIs used by the kind fixture. Configure:

- `InferencePool` endpoints to target adapter port 8200;
- Endpoint Picker Service to the Router gRPC Service;
- buffered request-body ext-proc mode;
- disabled response-body mode with response headers/metadata enabled;
- fail-open behavior for Router unavailability;
- one fixed model route.

- [ ] **Step 4: Write the reference Worker Pod**

Place adapter and pinned vLLM in the same Pod. Bind vLLM chat to loopback for adapter-only access; expose metrics and KV event ports only through explicit internal container ports plus NetworkPolicy allowing Router traffic. Configure prefix caching, KV event PUB/replay, fixed model identity, and adapter workload token. The published InferencePool endpoint is adapter port 8200, never the vLLM chat port.

- [ ] **Step 5: Document the deployment contract**

In `README.md`, record exact image/version pins, ports, one-model/text-only limits, one-KV-group restriction, Renderer identity invariants, fail-open semantics, security boundary, and the explicit absence of Kubernetes SDK/RBAC/controller/state replication.

- [ ] **Step 6: Validate and commit**

```bash
docker build -f docker/router/Dockerfile -t mini-router:test .
docker build -f docker/adapter/Dockerfile -t mini-router-adapter:test .
kubectl apply --dry-run=client -f deploy/k8s
rg -n 'Role|RoleBinding|Lease|kube::|k8s_openapi' deploy src Cargo.toml
```

Expected: builds and dry-run succeed; the final search returns no Router RBAC, Lease, or Kubernetes Rust dependency. Then commit:

```bash
git add docker deploy README.md
git commit -m "build: package router deployment"
```

---

### Task 13: Prove the complete path in kind

**Files:**
- Create: `tests/kind/cluster.yaml`
- Create: `tests/kind/namespace.yaml`
- Create: `tests/kind/gateway.yaml`
- Create: `tests/kind/router.yaml`
- Create: `tests/kind/worker.yaml`
- Create: `tests/kind/pool.yaml`
- Create: `tests/kind/route.yaml`
- Create: `tests/kind/setup.sh`
- Create: `tests/kind/verify.sh`
- Create: `tests/kind/teardown.sh`
- Create: `.github/workflows/ci.yaml`

**Interfaces:**
- Consumes: all project images and manifests.
- Produces: a repeatable no-GPU end-to-end acceptance suite.

- [ ] **Step 1: Pin and create the test cluster**

`setup.sh` must verify exact tool versions, create a kind 0.32 cluster from `cluster.yaml`, install pinned Gateway API, Envoy AI Gateway 0.6, and Inference Extension 1.5 manifests, load locally built Router/adapter/mock images, and apply the one-word YAML fixtures. Use shell `trap` in CI so teardown always deletes the uniquely named cluster.

- [ ] **Step 2: Deploy the real control/data path**

Run a real Envoy Gateway/AI Gateway, two real Router replicas, a real local vLLM Renderer pinned to `Qwen/Qwen2.5-0.5B-Instruct@7ae557604adf67be50417f59c2c2f167def9a775`, two real Rust adapters, and two mock vLLM engines. Membership changes must be made through the real InferencePool/EndpointSlice path; test scripts must never write candidates into Router memory.

- [ ] **Step 3: Verify baseline protocol and data path**

`verify.sh` must assert:

1. EPP health reports serving for liveness, readiness, and service name;
2. candidate subset reaches the Router;
3. selected endpoint header equals `envoy.lb` dynamic metadata;
4. Gateway applies body mutation and reaches the chosen adapter;
5. mock vLLM receives the exact Renderer IDs only in `prompt_token_ids`;
6. ordinary and SSE responses reach the client without aggregation.

- [ ] **Step 4: Verify routing state transitions**

Through mock network endpoints, publish ownership/load and assert cache-vs-load decisions. Then cover duplicate event, recoverable gap, unrecoverable gap, replay, disconnect/reconnect, generation isolation, Least Load degradation, lazy state creation for a newly added Worker, immediate ineligibility after membership removal, and TTL cleanup through exported metrics.

- [ ] **Step 5: Verify resilience and replica independence**

Assert served metadata releases a reservation before TTL while no response body reaches EPP. Scale Router to two replicas and show each can independently produce legal decisions. Make Router unavailable and assert Gateway fail-open reaches an adapter raw-fallback path where vLLM tokenizes exactly once.

- [ ] **Step 6: Add CI gates**

The workflow must run, in order:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo bench --bench router --no-run
docker build -f docker/router/Dockerfile -t mini-router:test .
docker build -f docker/adapter/Dockerfile -t mini-router-adapter:test .
docker build -f docker/mock/Dockerfile -t mini-router-mock:test .
tests/kind/setup.sh
tests/kind/verify.sh
```

Pin all GitHub Actions by immutable commit SHA and upload cluster logs on failure.

- [ ] **Step 7: Run and commit**

```bash
tests/kind/setup.sh
tests/kind/verify.sh
tests/kind/teardown.sh
git add tests/kind .github/workflows/ci.yaml
git commit -m "test: verify router in kind"
```

Expected: all assertions pass on a CPU-only development machine.

---

### Task 14: Add benchmarks and real-GPU release validation

**Files:**
- Create: `benches/router.rs`
- Create: `tests/gpu/worker.yaml`
- Create: `tests/gpu/verify.sh`
- Create: `tests/gpu/bench.sh`
- Modify: `README.md`

**Interfaces:**
- Consumes: completed deployment and test controls.
- Produces: repeatable performance comparison and release-gate evidence.

- [ ] **Step 1: Write deterministic Criterion benchmarks**

Benchmark index apply/remove/clear, prefix lookup at 1/16/128 candidates, and scorer latency for Least Load versus KV + Load Hybrid. Use fixed seeds and token arrays; measure allocations and throughput separately from network tests.

- [ ] **Step 2: Define the GPU fixture**

Pin vLLM commit `9c22668436a4d94aab87ea74a220e060415cf1d8` and model `Qwen/Qwen2.5-0.5B-Instruct` revision `7ae557604adf67be50417f59c2c2f167def9a775` in `worker.yaml`. Deploy two GPU Workers with prefix caching/events enabled, the real adapter, the same Renderer identity probe digest, and vendor device-plugin resources. Do not fake `nvidia.com/gpu` capacity.

- [ ] **Step 3: Verify token identity and no re-tokenization**

`verify.sh` must send fixed ordinary and streaming chat requests, capture Renderer IDs and adapter-forwarded IDs through redacted test-only digests, and assert equality. Enable vLLM debug counters/traces needed to prove the token-in path was used and prompt rendering/tokenization was not repeated in the Worker.

- [ ] **Step 4: Verify real KV-aware behavior**

Warm a long shared prefix on worker A, then send controlled follow-ups while varying worker load. Assert prefix ownership selects A when the configured formula predicts it, load can outweigh the cache bonus when expected, and all responses remain OpenAI-compatible.

- [ ] **Step 5: Compare only the two supported policies**

`bench.sh` runs Least Load and KV + Load Hybrid against the same prompt corpus/concurrency schedule, then writes machine-readable results for cache hit ratio, avoided prefill tokens, TTFT, throughput, load skew, Renderer latency, EPP latency, adapter overhead, and replay recovery time. Fail if required metrics are absent; do not silently substitute estimates.

- [ ] **Step 6: Run final repository verification**

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo bench --bench router --no-run
tests/kind/setup.sh
tests/kind/verify.sh
tests/kind/teardown.sh
```

On an authorized GPU cluster, also run:

```bash
kubectl apply -f tests/gpu/worker.yaml
tests/gpu/verify.sh
tests/gpu/bench.sh
```

- [ ] **Step 7: Check plan/spec invariants and commit**

```bash
rg -n 'TODO|TBD|placeholder|fake implementation|stub' src tests deploy docker benches README.md
find . -type f -not -path './.git/*' -print | awk -F/ '{name=$NF; sub(/^\./,"",name); sub(/\.[^.]+$/,"",name); if (name ~ /[-_]/) print $0}'
```

Expected: the placeholder scan is empty, every project-owned filename stem is one word, and no Kubernetes client dependency or public OpenAI Router handler exists. Record exact verification commands and image/model digests in `README.md`, then commit:

```bash
git add benches tests/gpu README.md
git commit -m "test: add release validation"
```
