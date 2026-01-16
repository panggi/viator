//! Connection command implementations.

use super::ParsedCommand;
use crate::Result;
use crate::error::{CommandError, StorageError};
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::MAX_DB_INDEX;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// SELECT index
pub fn cmd_select(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let index = cmd.get_u64(0)?;

        if index > MAX_DB_INDEX as u64 {
            return Err(StorageError::DbIndexOutOfRange.into());
        }

        client.set_db_index(index as u16);
        Ok(Frame::ok())
    })
}

/// CLIENT subcommand [arguments]
pub fn cmd_client(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "ID" => Ok(Frame::Integer(client.id() as i64)),
            "GETNAME" => match client.name() {
                Some(name) => Ok(Frame::Bulk(name.into())),
                None => Ok(Frame::Null),
            },
            "SETNAME" => {
                if cmd.args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "CLIENT SETNAME".to_string(),
                    }
                    .into());
                }
                let name = cmd.get_str(1)?;
                client.set_name(name.to_string());
                Ok(Frame::ok())
            }
            "LIST" => {
                let info = format!(
                    "id={} addr=127.0.0.1:0 fd=0 name={} age=0 idle=0 flags=N db={} sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\n",
                    client.id(),
                    client.name().unwrap_or_default(),
                    client.db_index()
                );
                Ok(Frame::Bulk(info.into()))
            }
            "INFO" => {
                let info = format!(
                    "id={}\naddr=127.0.0.1:0\nfd=0\nname={}\nage=0\nidle=0\nflags=N\ndb={}\nsub=0\npsub=0\nmulti=-1\n",
                    client.id(),
                    client.name().unwrap_or_default(),
                    client.db_index()
                );
                Ok(Frame::Bulk(info.into()))
            }
            "SETINFO" => {
                // Client library tracking - just acknowledge
                Ok(Frame::ok())
            }
            "REPLY" => {
                // Reply mode control
                Ok(Frame::ok())
            }
            "NO-EVICT" => {
                // CLIENT NO-EVICT ON|OFF
                // Mark client to be protected from eviction during OOM
                let state = if cmd.args.len() >= 2 {
                    cmd.get_str(1)?.to_uppercase()
                } else {
                    "ON".to_string()
                };
                match state.as_str() {
                    "ON" | "OFF" => Ok(Frame::ok()),
                    _ => Ok(Frame::Error(
                        "ERR Syntax error, CLIENT NO-EVICT ON|OFF".into(),
                    )),
                }
            }
            "NO-TOUCH" => {
                // CLIENT NO-TOUCH ON|OFF
                // Don't update last access time for keys
                let state = if cmd.args.len() >= 2 {
                    cmd.get_str(1)?.to_uppercase()
                } else {
                    "ON".to_string()
                };
                match state.as_str() {
                    "ON" | "OFF" => Ok(Frame::ok()),
                    _ => Ok(Frame::Error(
                        "ERR Syntax error, CLIENT NO-TOUCH ON|OFF".into(),
                    )),
                }
            }
            "KILL" => {
                // CLIENT KILL [ip:port | ID client-id | TYPE type | USER user | ADDR addr | ...]
                Ok(Frame::ok())
            }
            "PAUSE" => {
                // CLIENT PAUSE timeout [WRITE|ALL]
                Ok(Frame::ok())
            }
            "UNPAUSE" => Ok(Frame::ok()),
            "GETREDIR" => {
                // Return the client tracking redirection ID (-1 if not tracking)
                Ok(Frame::Integer(-1))
            }
            "TRACKINGINFO" => {
                // Return tracking info
                Ok(Frame::Array(vec![
                    Frame::Bulk("flags".into()),
                    Frame::Array(vec![Frame::Bulk("off".into())]),
                    Frame::Bulk("redirect".into()),
                    Frame::Integer(-1),
                    Frame::Bulk("prefixes".into()),
                    Frame::Array(vec![]),
                ]))
            }
            "CACHING" => {
                // CLIENT CACHING YES|NO - for client-side caching tracking
                if cmd.args.len() >= 2 {
                    let state = cmd.get_str(1)?.to_uppercase();
                    match state.as_str() {
                        "YES" | "NO" => Ok(Frame::ok()),
                        _ => Ok(Frame::Error(
                            "ERR Syntax error, CLIENT CACHING YES|NO".into(),
                        )),
                    }
                } else {
                    Ok(Frame::Error(
                        "ERR wrong number of arguments for 'client caching' command".into(),
                    ))
                }
            }
            "TRACKING" => {
                // CLIENT TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
                if cmd.args.len() >= 2 {
                    let state = cmd.get_str(1)?.to_uppercase();
                    match state.as_str() {
                        "ON" | "OFF" => Ok(Frame::ok()),
                        _ => Ok(Frame::Error(
                            "ERR Syntax error, CLIENT TRACKING ON|OFF [options]".into(),
                        )),
                    }
                } else {
                    Ok(Frame::Error(
                        "ERR wrong number of arguments for 'client tracking' command".into(),
                    ))
                }
            }
            _ => Err(CommandError::InvalidArgument {
                command: "CLIENT".to_string(),
                arg: subcommand,
            }
            .into()),
        }
    })
}

/// QUIT
pub fn cmd_quit(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        client.close();
        Ok(Frame::ok())
    })
}

// NOTE: AUTH and HELLO commands are handled specially by CommandExecutor
// (see executor.rs handle_auth/handle_hello) because they need access to
// server auth configuration. They are NOT registered in the command registry.

/// RESET
/// Reset the connection state
pub fn cmd_reset(
    _cmd: ParsedCommand,
    _db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        // Reset client state
        client.set_name(String::new());
        client.set_db_index(0);
        // Unsubscribe from all channels/patterns would go here

        Ok(Frame::Simple("RESET".to_string()))
    })
}
