# Stage 1: Builder
FROM rust:1.87-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependency compilation separately from source
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

# Build all release binaries in the workspace
RUN cargo build --release --workspace

# Stage 2: Runtime
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root system user
RUN groupadd --system statemaster && \
    useradd --system --gid statemaster --no-create-home statemaster

# Copy the three binaries produced by the workspace
COPY --from=builder /build/target/release/smdbd    /usr/local/bin/smdbd
COPY --from=builder /build/target/release/smdbctl  /usr/local/bin/smdbctl
COPY --from=builder /build/target/release/smash    /usr/local/bin/smash

# Data directory for WAL, heap, and index files
RUN mkdir -p /var/lib/statemaster/data && \
    chown -R statemaster:statemaster /var/lib/statemaster

# Wire protocol | Metrics / health | Reserved for replication
EXPOSE 7632 7633 7634

USER statemaster

ENTRYPOINT ["/usr/local/bin/smdbd"]
CMD ["--config", "/etc/statemaster/statemaster.toml"]
