# Building Viator

This document covers how to build Viator from source with various configurations.

## Prerequisites

- **Rust 1.85+** - Install via [rustup](https://rustup.rs/)
- **Cargo** - Included with Rust
- **C compiler** - For jemalloc (gcc, clang, or MSVC)

### macOS

```bash
xcode-select --install
```

### Ubuntu/Debian

```bash
sudo apt update
sudo apt install build-essential pkg-config
```

### Fedora/RHEL

```bash
sudo dnf groupinstall "Development Tools"
```

## Quick Build

```bash
# Clone the repository
git clone https://github.com/panggi/viator.git
cd viator

# Build in release mode (optimized)
cargo build --release

# Run tests
cargo test
```

## Build Profiles

### Debug Build (fast compile, slow runtime)

```bash
cargo build
./target/debug/viator
```

### Release Build (slow compile, fast runtime)

```bash
cargo build --release
./target/release/viator
```

### Release with Debug Info

For profiling with symbols:

```bash
RUSTFLAGS="-C debuginfo=2" cargo build --release
```

## Feature Flags

Viator supports several optional features:

| Feature | Description | Default |
|---------|-------------|---------|
| `tls` | TLS/SSL support via rustls | No |
| `telemetry` | OpenTelemetry tracing | No |

### Build with TLS

```bash
cargo build --release --features tls
```

### Build with Telemetry

```bash
cargo build --release --features telemetry
```

### Build with All Features

```bash
cargo build --release --all-features
```

### Minimal Build

```bash
cargo build --release --no-default-features
```

## Build Optimization

### LTO (Link Time Optimization)

For maximum performance (slower build):

```bash
CARGO_PROFILE_RELEASE_LTO=true cargo build --release
```

Or add to `Cargo.toml`:

```toml
[profile.release]
lto = true
```

### Target-Specific Optimization

Build for your specific CPU:

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### Smaller Binary Size

```bash
cargo build --release
strip target/release/viator  # Linux/macOS
```

## Cross-Compilation

### Linux (from macOS)

```bash
rustup target add x86_64-unknown-linux-gnu
cargo build --release --target x86_64-unknown-linux-gnu
```

### ARM64 (from x86)

```bash
rustup target add aarch64-unknown-linux-gnu
cargo build --release --target aarch64-unknown-linux-gnu
```

## Running Tests

### All Tests

```bash
cargo test
```

### Library Tests Only

```bash
cargo test --lib
```

### Specific Test

```bash
cargo test test_bloom_filter
```

### With Output

```bash
cargo test -- --nocapture
```

## Benchmarks

```bash
cargo bench
```

## Static Analysis

### Clippy (Linter)

```bash
cargo clippy --all-targets
```

### Format Check

```bash
cargo fmt --check
```

## Troubleshooting

### jemalloc Build Fails

If jemalloc compilation fails:

1. Ensure you have a C compiler installed
2. On macOS, run `xcode-select --install`
3. On Linux, install `build-essential` or equivalent

### Memory Warnings

The unsafe code warnings for memory metrics are expected:

```
warning: usage of an `unsafe` block
  --> src/server/metrics.rs
```

These are necessary for `getrusage()` system calls.

### Slow Build

First builds are slow due to dependency compilation. Subsequent builds are faster.

Use `cargo build` (debug) for development iteration, `cargo build --release` only for final testing.

## Output Binaries

After `cargo build --release`:

| Binary | Description |
|--------|-------------|
| `target/release/viator` | Main server |
| `target/release/viator-cli` | Command-line client |
| `target/release/viator-benchmark` | Performance benchmarking |
| `target/release/viator-check-vdb` | VDB file integrity checker |
| `target/release/viator-check-aof` | AOF file checker |
| `target/release/viator-sentinel` | High availability monitor |

## Install Locally

```bash
cargo install --path .
```

This installs all binaries to `~/.cargo/bin/`.
