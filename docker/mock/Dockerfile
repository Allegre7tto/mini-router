FROM rust:1.94-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build -p router --release --bins --all-features
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/mock /usr/local/bin/mock
USER nobody
EXPOSE 8000 5557 5558
ENTRYPOINT ["mock"]
