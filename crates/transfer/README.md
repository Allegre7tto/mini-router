# Transfer

Asynchronous registered-memory transfers over TCP, with bounded concurrency and reusable connections. RDMA is not implemented.

Remote descriptors support `to_string()` and `parse()`. Both peers must use protocol version 2.

Cancellation and timeouts drain submitted work before releasing local resources; they do not roll back remote writes. `Error::Uncertain` quarantines the target: its owner must deregister it before reuse.

```bash
cargo test -p transfer
```

The `bench` binary measures same-process loopback transfers, not cross-node performance.
