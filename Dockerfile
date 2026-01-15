# Viator Dockerfile
# Multi-stage build for minimal image size

# Build stage
FROM rust:1.85-bookworm AS builder

WORKDIR /build

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy source for dependency caching
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs

# Build dependencies only
RUN cargo build --release && rm -rf src

# Copy real source code
COPY src ./src
COPY benches ./benches

# Build the actual binary
RUN touch src/main.rs src/lib.rs && cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create viator user
RUN useradd -r -s /bin/false viator

# Create directories
RUN mkdir -p /data /etc/viator && chown viator:viator /data

# Copy binary
COPY --from=builder /build/target/release/viator /usr/local/bin/viator

# Copy default config
COPY viator.conf /etc/viator/viator.conf

# Set working directory
WORKDIR /data

# Switch to non-root user
USER viator

# Expose default port
EXPOSE 6379

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/viator", "--version"] || exit 1

# Default command
ENTRYPOINT ["/usr/local/bin/viator"]
CMD ["--config", "/etc/viator/viator.conf"]
