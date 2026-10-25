FROM rust:1.94-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY transfer ./transfer
RUN cargo build --release --bins --all-features
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/router /usr/local/bin/router
USER nobody
EXPOSE 9000 9001
ENTRYPOINT ["router"]
