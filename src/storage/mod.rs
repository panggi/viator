//! Storage engine for Redis data.
//!
//! This module provides the core storage layer including:
//! - In-memory key-value store
//! - TTL/expiration handling
//! - Database selection (0-15)

mod db;
mod expiry;

pub use db::{
    Database, Db, DbEntry, DbStats, MemoryManager, MemoryStatus, ServerAuth, SharedMemoryManager,
    SharedServerAuth, VectorSetExport,
};
pub use expiry::ExpiryManager;
