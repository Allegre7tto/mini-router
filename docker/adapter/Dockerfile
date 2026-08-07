FROM rust:1.94-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo build --release --bin vllm-token-in-adapter
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/vllm-token-in-adapter /usr/local/bin/adapter
USER nobody
EXPOSE 8200
ENTRYPOINT ["adapter"]
