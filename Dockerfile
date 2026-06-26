# syntax=docker/dockerfile:1

# Builder stage. Rust >= 1.88 is required (the codebase uses stabilized
# let-chains on edition 2024); pinned to a recent stable.
FROM rust:1.92-slim AS builder

# Build dependencies. libssl-dev is needed because the Postgres connector uses
# native-tls (OpenSSL).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/stratum

# Copy the whole workspace and build only the CLI binary (and its deps), not the
# test crates. The build context is kept small by .dockerignore (excludes
# target/, .git, node_modules).
COPY . .
RUN cargo build --release -p cli

# Runtime stage.
FROM debian:bookworm-slim

# Runtime dependencies: OpenSSL (native-tls) and CA certificates (TLS to
# databases / outbound HTTP from plugins).
RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Run as a non-root user.
RUN useradd --user-group --create-home stratum
WORKDIR /home/stratum

# Copy the built binary. The `cli` crate produces a binary named `cli`.
COPY --from=builder /usr/src/stratum/target/release/cli /usr/local/bin/stratum
RUN chmod +x /usr/local/bin/stratum

USER stratum

ENTRYPOINT ["/usr/local/bin/stratum"]
