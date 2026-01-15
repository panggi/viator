//! Transaction command implementations (MULTI, EXEC, DISCARD, WATCH, UNWATCH).
//!
//! ## Implementation Notes
//!
//! MULTI/EXEC/DISCARD work correctly for single-client transactions.
//!
//! WATCH/UNWATCH commands are implemented but note that cross-client WATCH
//! invalidation (where a key modified by client A aborts client B's transaction)
//! requires a global watch registry that hooks into all write operations.
//! This is marked as a TODO for future enhancement.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::Key;
use crate::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// MULTI - Start a transaction block.
///
/// Marks the start of a transaction block. Subsequent commands will be
/// queued for atomic execution by EXEC.
pub fn cmd_multi(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if client.is_in_transaction() {
            return Err(CommandError::NestedMulti.into());
        }
        client.start_transaction();
        Ok(Frame::ok())
    })
}

/// EXEC - Execute all commands issued after MULTI.
///
/// Note: This is a placeholder - the actual execution logic is handled
/// by the CommandExecutor. The executor checks for EXEC, takes the
/// queued commands, and executes them atomically.
pub fn cmd_exec(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if !client.is_in_transaction() {
            return Err(CommandError::ExecWithoutMulti.into());
        }

        // Check if transaction was aborted due to WATCH
        if client.is_transaction_aborted() {
            client.discard_transaction();
            return Ok(Frame::Null);
        }

        // The actual execution is handled by the executor
        // This should not be reached if executor handles EXEC properly
        Ok(Frame::empty_array())
    })
}

/// DISCARD - Discard all commands issued after MULTI.
///
/// Flushes all previously queued commands in a transaction and restores
/// the connection state to normal.
pub fn cmd_discard(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if !client.is_in_transaction() {
            return Err(CommandError::DiscardWithoutMulti.into());
        }
        client.discard_transaction();
        Ok(Frame::ok())
    })
}

/// WATCH key [key ...] - Watch the given keys for conditional execution.
///
/// Marks the given keys to be watched for conditional execution of a
/// transaction. The transaction will be aborted if any of the watched
/// keys are modified before EXEC.
pub fn cmd_watch(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if client.is_in_transaction() {
            return Err(CommandError::WatchInsideMulti.into());
        }

        for arg in &cmd.args {
            let key = Key::from(arg.clone());
            client.watch_key(key);
        }
        Ok(Frame::ok())
    })
}

/// UNWATCH - Forget about all watched keys.
///
/// Flushes all the previously watched keys for a transaction.
/// If EXEC or DISCARD is called, there is no need to manually call UNWATCH.
pub fn cmd_unwatch(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        client.unwatch_all();
        Ok(Frame::ok())
    })
}
