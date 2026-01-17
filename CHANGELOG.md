# Changelog

All notable changes to Viator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.16] - 2026-01-17

### Fixed
- **Dockerfile**: Correct binary name from `viator` to `viator-server`
- **docker-compose**: Healthcheck now uses PING command instead of `--version`
- **Code hardening**: Replace `unwrap()` with documented `unreachable!()` patterns
  - compact.rs: IntSet slice-to-array conversions
  - sentinel.rs: SystemTime::duration_since(UNIX_EPOCH) calls

### Changed
- **Documentation**: Update all `.md` files with correct binary names
- **Documentation**: Remove benchmark results (environment-specific)
- **Documentation**: Clarify memory safety claims (minimal documented unsafe blocks)

## [0.1.6] - 2026-01-16

### Added
- **LZF Compression**: VDB persistence now compresses strings >= 20 bytes
  - Reduces dump file sizes ~6x for benchmark/repetitive data
  - Matches Redis RDB compression behavior
  - Hash-based back-reference detection with 8KB sliding window

## [0.1.5] - 2026-01-16

### Fixed
- Fix binary smoke tests for llvm-cov CI environment
  - Tests now check `CARGO_TARGET_DIR` and multiple candidate paths
  - Supports `target/llvm-cov-target/` directory used by coverage tools

## [0.1.4] - 2026-01-16

### Fixed
- Fix clippy warnings for unused variables in pattern matching (`-D warnings` CI compliance)
  - `start` → `_start` in cluster slot range iteration
  - `old_score` → `_old_score` in sorted set NX check

## [0.1.3] - 2026-01-16

### Fixed
- Fix code formatting (cargo fmt)

## [0.1.2] - 2026-01-16

Complete Redis 8.4.0 feature parity release.

### Added
- **Stream Persistence**: Full AOF and VDB persistence for Stream data type
  - VDB format includes last_id and entries_added metadata for correct ID generation after restart
  - AOF format uses XADD with explicit IDs and XSETID for metadata preservation
- **Real CPU Metrics**: INFO CPU section now reports actual process CPU times
  - Uses `getrusage(RUSAGE_SELF)` and `getrusage(RUSAGE_CHILDREN)` for accurate system/user CPU time

### Fixed
- Stream data now survives server restart (previously lost on restart)
- INFO CPU no longer shows placeholder 0.000000 values

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
