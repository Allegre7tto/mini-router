# mini-router

KV-aware request routing and asynchronous memory transfers.

```text
crates/
  router/      Endpoint picker and token-in adapter
  transfer/    TCP transfer engine
tests/kind/    Integration tests
deploy/k8s/    Deployment examples
docker/        Container builds
docs/specs/    Specifications
```

```bash
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
```
