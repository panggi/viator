//! Redis command implementation.
//!
//! This module provides the command parsing, routing, and execution layer.

mod executor;
mod registry;
mod strings;
mod keys;
mod lists;
mod hashes;
mod sets;
mod sorted_sets;
mod server_cmds;
mod connection;
mod transactions;
mod bitmap;
mod hyperloglog;
mod streams;
mod scripting;
mod pubsub;
mod geo;
mod blocking;
mod cluster;
pub mod sentinel;

// Redis Stack module commands
mod bloom_cmds;
mod cuckoo_cmds;
mod cms_cmds;
mod topk_cmds;
mod tdigest_cmds;
mod ts_cmds;
mod json_cmds;
mod vectorset_cmds;
mod search_cmds;

pub use executor::CommandExecutor;
pub use registry::{Command, CommandRegistry};

use crate::error::{CommandError, Result};
use crate::protocol::Frame;
use bytes::Bytes;

/// Parsed command with name and arguments.
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    /// Command name (uppercase)
    pub name: String,
    /// Command arguments
    pub args: Vec<Bytes>,
}

impl ParsedCommand {
    /// Parse a command from a RESP frame.
    pub fn from_frame(frame: Frame) -> Result<Self> {
        let frames = match frame {
            Frame::Array(arr) if !arr.is_empty() => arr,
            _ => return Err(CommandError::SyntaxError.into()),
        };

        let mut iter = frames.into_iter();

        // First element is the command name
        let name_frame = iter.next().ok_or(CommandError::SyntaxError)?;
        let name_bytes = name_frame.to_bytes().ok_or(CommandError::SyntaxError)?;
        let name = std::str::from_utf8(&name_bytes)
            .map_err(|_| CommandError::SyntaxError)?
            .to_uppercase();

        // Rest are arguments
        let args: Vec<Bytes> = iter
            .map(|f| f.to_bytes().unwrap_or_default())
            .collect();

        Ok(Self { name, args })
    }

    /// Get the number of arguments (excluding command name).
    #[inline]
    pub fn arg_count(&self) -> usize {
        self.args.len()
    }

    /// Get an argument as bytes.
    #[inline]
    pub fn get_arg(&self, index: usize) -> Option<&Bytes> {
        self.args.get(index)
    }

    /// Get an argument as a string.
    pub fn get_str(&self, index: usize) -> Result<&str> {
        let bytes = self.args.get(index).ok_or_else(|| CommandError::WrongArity {
            command: self.name.clone(),
        })?;
        std::str::from_utf8(bytes).map_err(|_| CommandError::SyntaxError.into())
    }

    /// Get an argument as an i64.
    pub fn get_i64(&self, index: usize) -> Result<i64> {
        let s = self.get_str(index)?;
        s.parse().map_err(|_| CommandError::NotInteger.into())
    }

    /// Get an argument as a u64.
    pub fn get_u64(&self, index: usize) -> Result<u64> {
        let s = self.get_str(index)?;
        s.parse().map_err(|_| CommandError::NotInteger.into())
    }

    /// Get an argument as an f64.
    pub fn get_f64(&self, index: usize) -> Result<f64> {
        let s = self.get_str(index)?;
        s.parse().map_err(|_| CommandError::NotFloat.into())
    }

    /// Validate minimum argument count.
    pub fn require_args(&self, min: usize) -> Result<()> {
        if self.args.len() < min {
            Err(CommandError::WrongArity {
                command: self.name.clone(),
            }
            .into())
        } else {
            Ok(())
        }
    }

    /// Validate exact argument count.
    pub fn require_exact_args(&self, count: usize) -> Result<()> {
        if self.args.len() != count {
            Err(CommandError::WrongArity {
                command: self.name.clone(),
            }
            .into())
        } else {
            Ok(())
        }
    }

    /// Validate argument count is in range.
    pub fn require_args_range(&self, min: usize, max: usize) -> Result<()> {
        if self.args.len() < min || self.args.len() > max {
            Err(CommandError::WrongArity {
                command: self.name.clone(),
            }
            .into())
        } else {
            Ok(())
        }
    }
}

bitflags::bitflags! {
    /// Command flags for ACL and behavior.
    ///
    /// Uses bitflags for compact representation (1 byte vs 8 for booleans).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct CommandFlags: u8 {
        /// Command is read-only
        const READONLY = 1 << 0;
        /// Command modifies data
        const WRITE    = 1 << 1;
        /// Command is for admin use
        const ADMIN    = 1 << 2;
        /// Command may block the connection
        const BLOCKING = 1 << 3;
        /// Command uses no keys
        const NO_KEYS  = 1 << 4;
        /// Command is fast (O(1))
        const FAST     = 1 << 5;
        /// Command may load data
        const LOADING  = 1 << 6;
        /// Command is for pub/sub
        const PUBSUB   = 1 << 7;
    }
}

impl CommandFlags {
    /// Flags for read-only commands (readonly + fast).
    #[inline]
    pub const fn readonly() -> Self {
        Self::READONLY.union(Self::FAST)
    }

    /// Flags for write commands.
    #[inline]
    pub const fn write() -> Self {
        Self::WRITE
    }

    /// Flags for admin commands (admin + no_keys).
    #[inline]
    pub const fn admin() -> Self {
        Self::ADMIN.union(Self::NO_KEYS)
    }

    /// Flags for blocking commands (write + blocking).
    #[inline]
    pub const fn blocking() -> Self {
        Self::WRITE.union(Self::BLOCKING)
    }

    /// Flags for pub/sub commands (pubsub + no_keys).
    #[inline]
    pub const fn pubsub_cmd() -> Self {
        Self::PUBSUB.union(Self::NO_KEYS)
    }

    /// Check if the command uses no keys.
    #[inline]
    pub const fn no_keys(self) -> bool {
        self.contains(Self::NO_KEYS)
    }
}
