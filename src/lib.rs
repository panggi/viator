//! # Viator
//!
//! A high-performance, memory-safe key-value store.
//!
//! Viator is a complete server implementation with:
//! - Full RESP3 protocol support
//! - All standard data types (String, List, Hash, Set, Sorted Set)
//! - Stack modules (JSON, TimeSeries, Bloom, etc.)
//! - Persistence (AOF and VDB)
//! - Pub/Sub messaging
//! - Transactions
//! - Lua scripting (optional)
//!
//! ## Security Features
//!
//! - Memory-safe implementation eliminating buffer overflows
//! - Constant-time password comparison
//! - TLS support via rustls
//! - Resource limits to prevent DoS
//!
//! ## Example
//!
//! ```no_run
//! use viator::{Server, Config, Result};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let config = Config::default();
//!     let server = Arc::new(Server::new(config));
//!     server.run().await
//! }
//! ```

#![doc(html_root_url = "https://docs.rs/viator/0.1.0")]
#![forbid(unsafe_op_in_unsafe_fn)]
#![warn(
    clippy::all,
    rust_2018_idioms,
    trivial_casts,
    trivial_numeric_casts,
    unused_lifetimes,
    unused_qualifications
)]
// Lint suppressions: These are documented technical debt or intentional patterns.
// Priority: LOW - Code works correctly, these are style/optimization improvements.
#![allow(
    clippy::module_name_repetitions,
    clippy::await_holding_lock,         // Reviewed: parking_lot locks are sync-safe
    clippy::type_complexity,            // Complex types needed for async command returns
    clippy::should_implement_trait,     // from_str, as_ref, as_mut naming
    clippy::inherent_to_string,         // Custom to_string implementations
    clippy::inherent_to_string_shadow_display,
    clippy::manual_strip,               // Clarity over brevity in parser code
    clippy::needless_range_loop,        // Index access needed in some algorithms
    clippy::vec_init_then_push,         // Some vectors built conditionally
    clippy::double_must_use,
    clippy::approx_constant,            // PI approximation in geo calculations
    clippy::manual_clamp,               // Clarity in bounds checking
    clippy::sliced_string_as_bytes,     // as_bytes after slice is intentional
    clippy::wrong_self_convention,      // to_* with Copy types
    clippy::empty_line_after_doc_comments,
    missing_docs, // Public API docs needed - future enhancement
    dead_code     // Some fields/methods reserved for future use
)]

// Use jemalloc for better performance on Unix systems
// Disabled under Miri since it cannot interpret jemalloc's foreign functions
#[cfg(all(not(target_env = "msvc"), not(miri), feature = "server"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// ─────────────────────────────────────────────────────────────────────────────
// Modules
// ─────────────────────────────────────────────────────────────────────────────

/// Command parsing and execution layer.
pub mod commands;
/// Error types and result aliases.
pub mod error;
/// VDB and AOF persistence.
pub mod persistence;
/// Buffer pooling for memory reuse.
pub mod pool;
/// RESP2/RESP3 protocol implementation.
pub mod protocol;
/// Access control, TLS, and audit logging.
pub mod security;
/// TCP server and connection management.
pub mod server;
/// In-memory key-value storage engine.
pub mod storage;
/// Redis data type implementations.
pub mod types;

/// OpenTelemetry integration (optional).
#[cfg(feature = "telemetry")]
pub mod telemetry;

// ─────────────────────────────────────────────────────────────────────────────
// Common Re-exports
// ─────────────────────────────────────────────────────────────────────────────

// Error handling
pub use error::{Error, Result};

// Protocol
pub use protocol::{Frame, RespParser};

// Server
pub use server::{Config, Server};

// Storage
pub use storage::Database;

// Types
pub use types::{Key, ViatorValue};

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Crate version (from Cargo.toml).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Redis protocol version for compatibility.
pub const REDIS_VERSION: &str = "8.4.0";

/// Default server port.
pub const DEFAULT_PORT: u16 = 6379;

/// Maximum inline request size (64 KiB).
pub const MAX_INLINE_SIZE: usize = 64 * 1024;

/// Maximum bulk string size (512 MiB).
pub const MAX_BULK_SIZE: usize = 512 * 1024 * 1024;

/// Maximum number of arguments in a command.
pub const MAX_ARGUMENTS: usize = 1_000_000;

/// Maximum number of concurrent clients.
pub const MAX_CLIENTS: usize = 10_000;
