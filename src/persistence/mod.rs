//! Persistence layer for Viator data.
//!
//! This module implements two persistence mechanisms:
//! - **VDB**: Point-in-time snapshots in a compact binary format (Viator Database)
//! - **AOF**: Append-only file for durability with configurable fsync
//!
//! # Architecture
//!
//! The persistence layer is designed to be non-blocking. Background saves
//! are performed asynchronously while the server continues to handle requests.

mod aof;
mod vdb;

pub use aof::{AofFsync, AofReader, AofWriter};
pub use vdb::{VdbLoader, VdbSaver};

use crate::error::StorageError;
use crate::storage::Database;
use std::path::Path;
use std::sync::Arc;

/// Configuration for persistence.
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// VDB file path
    pub vdb_path: Option<String>,
    /// AOF file path
    pub aof_path: Option<String>,
    /// AOF fsync policy
    pub aof_fsync: AofFsync,
    /// VDB save points (seconds, changes)
    pub save_points: Vec<(u64, u64)>,
    /// Use VDB preamble in AOF
    pub aof_use_vdb_preamble: bool,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            vdb_path: Some("dump.vdb".to_string()),
            aof_path: None,
            aof_fsync: AofFsync::EverySec,
            // Default save points: after 1 hour if >= 1 change, 5 min if >= 100, 1 min if >= 10000
            save_points: vec![(3600, 1), (300, 100), (60, 10000)],
            aof_use_vdb_preamble: true,
        }
    }
}

/// Persistence manager that coordinates VDB and AOF operations.
pub struct PersistenceManager {
    config: PersistenceConfig,
    database: Arc<Database>,
    aof_writer: Option<AofWriter>,
}

impl PersistenceManager {
    /// Create a new persistence manager.
    pub fn new(config: PersistenceConfig, database: Arc<Database>) -> Self {
        Self {
            config,
            database,
            aof_writer: None,
        }
    }

    /// Initialize persistence (load existing data).
    pub async fn initialize(&mut self) -> Result<(), StorageError> {
        // Load VDB if it exists
        if let Some(ref path) = self.config.vdb_path {
            if Path::new(path).exists() {
                tracing::info!("Loading VDB file: {}", path);
                let loader = VdbLoader::new(path)?;
                loader.load_into(&self.database)?;
                tracing::info!("VDB loaded successfully");
            }
        }

        // Initialize AOF writer if enabled
        if let Some(ref path) = self.config.aof_path {
            let writer = AofWriter::new(path, self.config.aof_fsync)?;
            self.aof_writer = Some(writer);
            tracing::info!("AOF enabled: {}", path);
        }

        Ok(())
    }

    /// Trigger a background VDB save.
    /// Uses spawn_blocking to avoid blocking the async runtime.
    pub async fn save_vdb(&self) -> Result<(), StorageError> {
        if let Some(ref path) = self.config.vdb_path {
            let path = path.clone();
            let database = self.database.clone();

            tokio::task::spawn_blocking(move || {
                let saver = VdbSaver::new(&path)?;
                saver.save(&database)?;
                tracing::info!("VDB saved to: {}", path);
                Ok::<(), StorageError>(())
            })
            .await
            .map_err(|e| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("spawn_blocking panicked: {}", e)
            )))??;
        }
        Ok(())
    }

    /// Append a command to AOF.
    pub fn aof_append(&mut self, command: &[u8]) -> Result<(), StorageError> {
        if let Some(ref mut writer) = self.aof_writer {
            writer.append(command)?;
        }
        Ok(())
    }

    /// Force AOF fsync.
    pub fn aof_fsync(&mut self) -> Result<(), StorageError> {
        if let Some(ref mut writer) = self.aof_writer {
            writer.fsync()?;
        }
        Ok(())
    }

    /// Rewrite AOF file (BGREWRITEAOF).
    /// Uses spawn_blocking to avoid blocking the async runtime.
    pub async fn rewrite_aof(&self) -> Result<(), StorageError> {
        if let Some(ref path) = self.config.aof_path {
            let path = path.clone();
            let database = self.database.clone();
            let use_vdb_preamble = self.config.aof_use_vdb_preamble;

            tokio::task::spawn_blocking(move || {
                // Write current state to a temp file
                let temp_path = format!("{}.tmp", path);

                if use_vdb_preamble {
                    // Use VDB preamble for faster loading
                    let saver = VdbSaver::new(&temp_path)?;
                    saver.save(&database)?;
                } else {
                    // Write as RESP commands
                    AofWriter::rewrite_from_db(&temp_path, &database)?;
                }

                // Atomically rename
                std::fs::rename(&temp_path, &path).map_err(StorageError::Io)?;
                tracing::info!("AOF rewritten: {}", path);
                Ok::<(), StorageError>(())
            })
            .await
            .map_err(|e| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("spawn_blocking panicked: {}", e)
            )))??;
        }
        Ok(())
    }
}
