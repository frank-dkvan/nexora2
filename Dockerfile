# ── Builder stage ──────────────────────────────────────────────
# Use a Rust version that matches the workspace's rust-version (>= 1.88).
FROM rust:1.88-slim-bookworm AS builder

# Install build dependencies: pkg-config, openssl headers, and curl for healthcheck.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace manifest and lockfile first for layer caching.
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY fbs/ fbs/

# Build the release binary.
RUN cargo build --release -p nexora-app

# ── Runtime stage ──────────────────────────────────────────────
FROM debian:bookworm-slim

# Install runtime dependencies: ca-certificates for TLS, curl for healthcheck.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary.
COPY --from=builder /app/target/release/nexora-app /usr/local/bin/nexora-app

# Create data directory.
RUN mkdir -p /data

EXPOSE 8080
# When TLS is enabled via --tls-cert/--tls-key, also expose 8443
EXPOSE 8443

# Health check using curl (now available in the runtime image).
# Uses HTTP on port 8080; if TLS is enabled, healthcheck runs on the same port.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sfk http://localhost:8080/api/v2/health || exit 1

# Usage examples:
#   HTTP:  docker run -p 8080:8080 nexora-app
#   HTTPS: docker run -p 8443:8443 nexora-app --tls-cert /certs/cert.pem --tls-key /certs/key.pem
#   Self-signed: docker run nexora-app --gen-tls-cert /certs

ENTRYPOINT ["nexora-app"]
CMD ["--host", "0.0.0.0", "--port", "8080", "--rocksdb-path", "/data", "--wal-dir", "/data/wal"]
