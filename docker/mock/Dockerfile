FROM rust:1.94-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
RUN cargo build --release --features mock --bin mock
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libzmq3-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/mock /usr/local/bin/mock
USER nobody
EXPOSE 8000 5557 5558
ENTRYPOINT ["mock"]
