#!/bin/bash
# Profile-Guided Optimization (PGO) build script for Viator
#
# PGO improves performance by optimizing based on real workload profiles.
# Expected improvement: 5-15% depending on workload patterns.
#
# Usage:
#   ./scripts/pgo-build.sh              # Full PGO build with Redis workload
#   ./scripts/pgo-build.sh --quick      # Quick PGO with minimal profiling
#
# Requirements:
#   - Rust nightly (for PGO support)
#   - redis-benchmark (for profiling workload)
#   - llvm-profdata (usually installed with LLVM)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TARGET_DIR="$PROJECT_DIR/target"
PROFILE_DIR="$TARGET_DIR/pgo-profiles"
MERGED_PROFILE="$PROFILE_DIR/merged.profdata"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Check for required tools
check_requirements() {
    info "Checking requirements..."

    if ! command -v cargo &> /dev/null; then
        error "cargo not found. Please install Rust."
    fi

    if ! command -v llvm-profdata &> /dev/null; then
        warn "llvm-profdata not found. Trying to find it..."
        # Try common locations
        for path in /usr/local/opt/llvm/bin /opt/homebrew/opt/llvm/bin /usr/lib/llvm-*/bin; do
            if [ -f "$path/llvm-profdata" ]; then
                export PATH="$path:$PATH"
                info "Found llvm-profdata at $path"
                break
            fi
        done
        if ! command -v llvm-profdata &> /dev/null; then
            error "llvm-profdata not found. Please install LLVM."
        fi
    fi

    success "All requirements met."
}

# Clean previous profiles
clean_profiles() {
    info "Cleaning previous profile data..."
    rm -rf "$PROFILE_DIR"
    mkdir -p "$PROFILE_DIR"
}

# Step 1: Build instrumented binary
build_instrumented() {
    info "Building instrumented binary for profiling..."

    RUSTFLAGS="-Cprofile-generate=$PROFILE_DIR" cargo build \
        --release \
        --target-dir="$TARGET_DIR/pgo-instrumented"

    success "Instrumented binary built."
}

# Step 2: Run profiling workload
run_profiling() {
    local quick=$1
    info "Running profiling workload..."

    local VIATOR_BIN="$TARGET_DIR/pgo-instrumented/release/viator"
    local PORT=16379  # Use non-standard port to avoid conflicts

    # Start viator in background
    "$VIATOR_BIN" --port $PORT &
    local VIATOR_PID=$!

    # Wait for server to start
    sleep 2

    # Check if server is running
    if ! kill -0 $VIATOR_PID 2>/dev/null; then
        error "Failed to start Viator server for profiling."
    fi

    info "Viator started with PID $VIATOR_PID"

    # Run profiling workload
    if command -v redis-benchmark &> /dev/null; then
        if [ "$quick" = "true" ]; then
            info "Running quick profiling workload..."
            redis-benchmark -p $PORT -t set,get -n 10000 -q || true
        else
            info "Running full profiling workload..."
            # Comprehensive workload covering different command types
            redis-benchmark -p $PORT -t set -n 100000 -d 64 -q || true
            redis-benchmark -p $PORT -t get -n 100000 -q || true
            redis-benchmark -p $PORT -t incr -n 50000 -q || true
            redis-benchmark -p $PORT -t lpush -n 50000 -q || true
            redis-benchmark -p $PORT -t rpush -n 50000 -q || true
            redis-benchmark -p $PORT -t lpop -n 50000 -q || true
            redis-benchmark -p $PORT -t rpop -n 50000 -q || true
            redis-benchmark -p $PORT -t sadd -n 50000 -q || true
            redis-benchmark -p $PORT -t hset -n 50000 -q || true
            redis-benchmark -p $PORT -t spop -n 50000 -q || true
            redis-benchmark -p $PORT -t zadd -n 50000 -q || true
            redis-benchmark -p $PORT -t zpopmin -n 50000 -q || true
            redis-benchmark -p $PORT -t lrange -n 10000 -q || true
            redis-benchmark -p $PORT -t mset -n 10000 -q || true
            # Pipeline tests
            redis-benchmark -p $PORT -t set,get -n 100000 -P 16 -q || true
        fi
    else
        warn "redis-benchmark not found. Using basic workload..."
        # Basic workload without redis-benchmark
        for i in $(seq 1 1000); do
            echo "SET key$i value$i" | nc -q1 localhost $PORT > /dev/null 2>&1 || true
            echo "GET key$i" | nc -q1 localhost $PORT > /dev/null 2>&1 || true
        done
    fi

    # Graceful shutdown
    info "Stopping Viator..."
    kill $VIATOR_PID 2>/dev/null || true
    wait $VIATOR_PID 2>/dev/null || true

    # Give time for profile data to be written
    sleep 1

    success "Profiling workload completed."
}

# Step 3: Merge profile data
merge_profiles() {
    info "Merging profile data..."

    local profiles=("$PROFILE_DIR"/*.profraw)
    if [ ! -e "${profiles[0]}" ]; then
        error "No profile data found. Profiling may have failed."
    fi

    llvm-profdata merge -o "$MERGED_PROFILE" "$PROFILE_DIR"/*.profraw

    success "Profile data merged: $MERGED_PROFILE"
}

# Step 4: Build optimized binary
build_optimized() {
    info "Building PGO-optimized binary..."

    RUSTFLAGS="-Cprofile-use=$MERGED_PROFILE -Cllvm-args=-pgo-warn-missing-function" \
        cargo build --release

    success "PGO-optimized binary built at $TARGET_DIR/release/viator"
}

# Verify the build
verify_build() {
    info "Verifying build..."

    local VIATOR_BIN="$TARGET_DIR/release/viator"

    if [ ! -f "$VIATOR_BIN" ]; then
        error "Binary not found at $VIATOR_BIN"
    fi

    # Check binary size
    local size=$(ls -lh "$VIATOR_BIN" | awk '{print $5}')
    info "Binary size: $size"

    # Quick smoke test
    "$VIATOR_BIN" --version

    success "Build verification passed!"
}

# Main execution
main() {
    local quick=false

    if [ "$1" = "--quick" ]; then
        quick=true
        info "Quick PGO build mode enabled."
    fi

    echo ""
    echo "=========================================="
    echo "  Viator PGO Build"
    echo "=========================================="
    echo ""

    cd "$PROJECT_DIR"

    check_requirements
    clean_profiles
    build_instrumented
    run_profiling "$quick"
    merge_profiles
    build_optimized
    verify_build

    echo ""
    echo "=========================================="
    echo "  PGO Build Complete!"
    echo "=========================================="
    echo ""
    echo "Optimized binary: $TARGET_DIR/release/viator"
    echo ""
    echo "Run benchmarks to verify improvement:"
    echo "  redis-benchmark -t set,get -n 100000 -q"
    echo ""
}

main "$@"
