# Changelog

All notable changes to Viator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-15

Initial release with full Redis 8.4.0 protocol compatibility.

### Added
- Complete Redis 8.4.0 protocol compatibility
- Core data types: String, List, Hash, Set, Sorted Set, Stream
- VectorSet data type with HNSW-based similarity search
- Redis Stack modules: JSON, TimeSeries, Bloom, Cuckoo, TopK, T-Digest, CMS
- Cluster mode with automatic failover
- Sentinel monitoring and high availability
- Lua 5.4 scripting with real SHA1 hashing
- ACL system with user permissions
- TLS 1.3 support via rustls
- Persistence: AOF and VDB with CRC64 checksums, LZF compression
- LRU eviction with Redis-style 5-key sampling
- Pub/Sub messaging
- Transactions (MULTI/EXEC)

### Security
- Memory-safe implementation in Rust
- Constant-time password comparison
- AUTH rate limiting (10 failures → 60-second lockout)
- Sandboxed Lua execution
