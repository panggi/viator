//! Top-K command implementations.
//!
//! Redis Stack compatible TOPK.* commands.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::TopK;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Global storage for Top-K structures.
static TOPK_STRUCTURES: std::sync::LazyLock<RwLock<HashMap<Vec<u8>, TopK>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// TOPK.RESERVE key topk [width depth decay]
/// Reserve a Top-K structure.
pub fn cmd_topk_reserve(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();
        let k = cmd.get_u64(1)? as usize;

        let width = if cmd.args.len() > 2 {
            cmd.get_u64(2)? as usize
        } else {
            8 * k
        };

        let depth = if cmd.args.len() > 3 {
            cmd.get_u64(3)? as usize
        } else {
            7
        };

        let decay: f64 = if cmd.args.len() > 4 {
            cmd.get_str(4)?
                .parse()
                .map_err(|_| CommandError::NotFloat)?
        } else {
            0.9
        };

        let mut structures = TOPK_STRUCTURES.write();
        if structures.contains_key(&key) {
            return Err(CommandError::KeyExists.into());
        }

        structures.insert(key, TopK::new(k, width, depth, decay));
        Ok(Frame::ok())
    })
}

/// TOPK.ADD key item [item ...]
/// Add items to a Top-K structure.
pub fn cmd_topk_add(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let mut structures = TOPK_STRUCTURES.write();
        let topk = structures
            .entry(key)
            .or_insert_with(|| TopK::new(10, 80, 7, 0.9));

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|item| {
                let evicted = topk.add(item, 1);
                match evicted {
                    Some(evicted_item) => Frame::Bulk(evicted_item),
                    None => Frame::Null,
                }
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TOPK.INCRBY key item increment [item increment ...]
/// Increment item counts in a Top-K structure.
pub fn cmd_topk_incrby(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        if (cmd.args.len() - 1) % 2 != 0 {
            return Err(CommandError::WrongArity {
                command: "TOPK.INCRBY".to_string(),
            }
            .into());
        }

        let key = cmd.args[0].to_vec();

        let mut structures = TOPK_STRUCTURES.write();
        let topk = structures
            .entry(key)
            .or_insert_with(|| TopK::new(10, 80, 7, 0.9));

        let mut results = Vec::new();
        let mut i = 1;
        while i < cmd.args.len() {
            let item = &cmd.args[i];
            let increment = cmd.get_u64(i + 1)?;
            let evicted = topk.add(item, increment);
            match evicted {
                Some(evicted_item) => results.push(Frame::Bulk(evicted_item)),
                None => results.push(Frame::Null),
            }
            i += 2;
        }

        Ok(Frame::Array(results))
    })
}

/// TOPK.QUERY key item [item ...]
/// Check if items are in the Top-K.
pub fn cmd_topk_query(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let structures = TOPK_STRUCTURES.read();
        let topk = structures.get(&key);

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|item| {
                // query returns count; if > 0, the item is tracked
                let count = topk.map(|t| t.query(item)).unwrap_or(0);
                Frame::Integer(if count > 0 { 1 } else { 0 })
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TOPK.COUNT key item [item ...]
/// Get estimated counts for items.
pub fn cmd_topk_count(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let structures = TOPK_STRUCTURES.read();
        let topk = structures.get(&key);

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|item| {
                let count = topk.map(|t| t.query(item)).unwrap_or(0);
                Frame::Integer(count as i64)
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// TOPK.LIST key [WITHCOUNT]
/// List the top-k items.
pub fn cmd_topk_list(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;
        let key = cmd.args[0].to_vec();

        let with_count = cmd.args.len() > 1 && cmd.get_str(1)?.to_uppercase() == "WITHCOUNT";

        let structures = TOPK_STRUCTURES.read();
        let topk = structures.get(&key).ok_or(CommandError::NoSuchKey)?;

        let items = topk.list();

        if with_count {
            let results: Vec<Frame> = items
                .into_iter()
                .flat_map(|(item, count)| vec![Frame::Bulk(item), Frame::Integer(count as i64)])
                .collect();
            Ok(Frame::Array(results))
        } else {
            let results: Vec<Frame> = items
                .into_iter()
                .map(|(item, _)| Frame::Bulk(item))
                .collect();
            Ok(Frame::Array(results))
        }
    })
}

/// TOPK.INFO key
/// Get information about a Top-K structure.
pub fn cmd_topk_info(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;
        let key = cmd.args[0].to_vec();

        let structures = TOPK_STRUCTURES.read();
        let topk = structures.get(&key).ok_or(CommandError::NoSuchKey)?;

        let info = topk.info();
        Ok(Frame::Array(vec![
            Frame::bulk("k"),
            Frame::Integer(info.k as i64),
            Frame::bulk("width"),
            Frame::Integer(info.width as i64),
            Frame::bulk("depth"),
            Frame::Integer(info.depth as i64),
            Frame::bulk("decay"),
            Frame::Bulk(format!("{}", info.decay).into()),
        ]))
    })
}
