//! Smoke tests for all Viator binaries.
//!
//! These tests verify that each binary can be executed and responds to --help.
//! They are designed to catch build-time linking issues and basic functionality.

use std::process::Command;

fn get_binary_path(name: &str) -> String {
    // Check multiple possible locations for the binary
    // Priority: CARGO_TARGET_DIR env var, llvm-cov-target, release, debug
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());

    let candidates = [
        format!("{}/release/{}", target_dir, name),
        format!("{}/debug/{}", target_dir, name),
        // Fallback paths for when CARGO_TARGET_DIR isn't set but llvm-cov is used
        format!("target/llvm-cov-target/release/{}", name),
        format!("target/llvm-cov-target/debug/{}", name),
        format!("target/release/{}", name),
        format!("target/debug/{}", name),
    ];

    for path in &candidates {
        if std::path::Path::new(path).exists() {
            return path.clone();
        }
    }

    // Return the most likely path for better error messages
    format!("{}/debug/{}", target_dir, name)
}

/// Test that viator-server binary responds to --help
#[test]
fn test_viator_help() {
    let output = Command::new(get_binary_path("viator-server"))
        .arg("--help")
        .output()
        .expect("Failed to execute viator-server");

    assert!(output.status.success(), "viator-server --help failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("viator-server") || stdout.contains("USAGE"),
        "viator-server --help output unexpected: {}",
        stdout
    );
}

/// Test that viator-server binary responds to --version
#[test]
fn test_viator_server_version() {
    let output = Command::new(get_binary_path("viator-server"))
        .arg("--version")
        .output()
        .expect("Failed to execute viator-server");

    assert!(output.status.success(), "viator-server --version failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("0.1") || stdout.contains("viator"),
        "viator-server --version output unexpected: {}",
        stdout
    );
}

/// Test that viator-cli binary responds to --help
#[test]
fn test_viator_cli_help() {
    let output = Command::new(get_binary_path("viator-cli"))
        .arg("--help")
        .output()
        .expect("Failed to execute viator-cli");

    assert!(output.status.success(), "viator-cli --help failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Usage") || stdout.contains("viator-cli"),
        "viator-cli --help output unexpected: {}",
        stdout
    );
}

/// Test that viator-sentinel binary responds to --help
#[test]
fn test_viator_sentinel_help() {
    let output = Command::new(get_binary_path("viator-sentinel"))
        .arg("--help")
        .output()
        .expect("Failed to execute viator-sentinel");

    assert!(output.status.success(), "viator-sentinel --help failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Sentinel") || stdout.contains("Usage"),
        "viator-sentinel --help output unexpected: {}",
        stdout
    );
}

/// Test that viator-benchmark binary responds to --help
#[test]
fn test_viator_benchmark_help() {
    let output = Command::new(get_binary_path("viator-benchmark"))
        .arg("--help")
        .output()
        .expect("Failed to execute viator-benchmark");

    assert!(output.status.success(), "viator-benchmark --help failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Usage") || stdout.contains("benchmark"),
        "viator-benchmark --help output unexpected: {}",
        stdout
    );
}

/// Test that viator-check-aof binary responds to --help
#[test]
fn test_viator_check_aof_help() {
    let output = Command::new(get_binary_path("viator-check-aof"))
        .arg("--help")
        .output()
        .expect("Failed to execute viator-check-aof");

    assert!(output.status.success(), "viator-check-aof --help failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Usage") || stdout.contains("AOF"),
        "viator-check-aof --help output unexpected: {}",
        stdout
    );
}

/// Test that viator-check-vdb binary responds to --help
#[test]
fn test_viator_check_vdb_help() {
    let output = Command::new(get_binary_path("viator-check-vdb"))
        .arg("--help")
        .output()
        .expect("Failed to execute viator-check-vdb");

    assert!(output.status.success(), "viator-check-vdb --help failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Usage") || stdout.contains("VDB"),
        "viator-check-vdb --help output unexpected: {}",
        stdout
    );
}

/// Test that all binaries are consistent in their help format
#[test]
fn test_all_binaries_have_help() {
    let binaries = [
        "viator-server",
        "viator-cli",
        "viator-sentinel",
        "viator-benchmark",
        "viator-check-aof",
        "viator-check-vdb",
    ];

    for name in binaries {
        let output = Command::new(get_binary_path(name))
            .arg("--help")
            .output()
            .unwrap_or_else(|_| panic!("Failed to execute {}", name));

        assert!(
            output.status.success(),
            "{} --help failed with status: {:?}",
            name,
            output.status
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(!stdout.is_empty(), "{} --help produced empty output", name);
    }
}
