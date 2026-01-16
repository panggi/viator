# Changelog

All notable changes to Viator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-01-16

Production hardening release.

### Changed
- Replace `std::sync::RwLock` with `parking_lot::RwLock` in sentinel to prevent lock poisoning on panic
- Add connection semaphore for proper backpressure instead of just atomic counter checks
- Convert bare `unwrap()` calls to documented `expect()` in production code

### Fixed
- Dockerfile HEALTHCHECK now uses PING command instead of --version
- Fixed clippy `expect_fun_call` warning in binary smoke tests

### Added
- MSRV (1.85) check in CI workflow
- SAFETY documentation for all unsafe blocks
- Binary smoke tests for all executables
- Fuzz testing targets for RESP parser and commands

### Security
- Connection backpressure prevents resource exhaustion under load
- Panic-safe locks prevent sentinel from entering poisoned state

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
