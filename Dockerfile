FROM rust:1.87.0-slim AS builder

# Install necessary tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Create app directory and copy Cargo files, so dependencies can be cached
WORKDIR /usr/src/stratum
COPY . .

# Build the actual application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install necessary runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3 && \
    rm -rf /var/lib/apt/lists/*

# Create a user to run the application
RUN useradd --user-group --create-home stratum
WORKDIR /home/stratum

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/stratum/target/release/cli /usr/local/bin/stratum

# Make the binary executable
RUN chmod +x /usr/local/bin/stratum && \
    chown stratum:stratum /usr/local/bin/stratum

# Switch to the non-root user
USER stratum

# Set the entrypoint to the binary
ENTRYPOINT ["/usr/local/bin/stratum"]
