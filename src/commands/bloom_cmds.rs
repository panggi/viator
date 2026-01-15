//! Bloom filter command implementations.
//!
//! Redis Stack compatible BF.* commands.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::ScalingBloomFilter;
use crate::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Global storage for bloom filters (separate from main key-value store).
/// In a production implementation, this would be integrated into the main storage.
static BLOOM_FILTERS: std::sync::LazyLock<RwLock<HashMap<Vec<u8>, ScalingBloomFilter>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// BF.ADD key item
/// Add an item to a Bloom filter.
pub fn cmd_bf_add(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;
        let key = cmd.args[0].to_vec();
        let item = &cmd.args[1];

        let mut filters = BLOOM_FILTERS.write();
        let filter = filters
            .entry(key)
            .or_insert_with(|| ScalingBloomFilter::new(1000, 0.01, 2));

        let added = filter.add(item);
        Ok(Frame::Integer(if added { 1 } else { 0 }))
    })
}

/// BF.MADD key item [item ...]
/// Add multiple items to a Bloom filter.
pub fn cmd_bf_madd(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let mut filters = BLOOM_FILTERS.write();
        let filter = filters
            .entry(key)
            .or_insert_with(|| ScalingBloomFilter::new(1000, 0.01, 2));

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|item| {
                let added = filter.add(item);
                Frame::Integer(if added { 1 } else { 0 })
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// BF.EXISTS key item
/// Check if an item exists in a Bloom filter.
pub fn cmd_bf_exists(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;
        let key = cmd.args[0].to_vec();
        let item = &cmd.args[1];

        let filters = BLOOM_FILTERS.read();
        let exists = filters
            .get(&key)
            .map(|f| f.contains(item))
            .unwrap_or(false);

        Ok(Frame::Integer(if exists { 1 } else { 0 }))
    })
}

/// BF.MEXISTS key item [item ...]
/// Check if multiple items exist in a Bloom filter.
pub fn cmd_bf_mexists(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let filters = BLOOM_FILTERS.read();
        let filter = filters.get(&key);

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|item| {
                let exists = filter.map(|f| f.contains(item)).unwrap_or(false);
                Frame::Integer(if exists { 1 } else { 0 })
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
/// Create a Bloom filter with specific parameters.
pub fn cmd_bf_reserve(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();
        let error_rate: f64 = cmd.get_str(1)?.parse().map_err(|_| CommandError::NotFloat)?;
        let capacity: usize = cmd.get_u64(2)? as usize;

        // Parse optional arguments
        let mut expansion = 2u32;
        let mut i = 3;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "EXPANSION" => {
                    i += 1;
                    if i < cmd.args.len() {
                        expansion = cmd.get_u64(i)? as u32;
                    }
                }
                "NONSCALING" => {
                    expansion = 1; // No scaling
                }
                _ => {}
            }
            i += 1;
        }

        let mut filters = BLOOM_FILTERS.write();
        if filters.contains_key(&key) {
            return Err(CommandError::KeyExists.into());
        }

        filters.insert(key, ScalingBloomFilter::new(capacity, error_rate, expansion));
        Ok(Frame::ok())
    })
}

/// BF.INFO key
/// Get information about a Bloom filter.
pub fn cmd_bf_info(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;
        let key = cmd.args[0].to_vec();

        let filters = BLOOM_FILTERS.read();
        let filter = filters.get(&key).ok_or(CommandError::NoSuchKey)?;

        let info = filter.info();
        Ok(Frame::Array(vec![
            Frame::bulk("Capacity"),
            Frame::Integer(info.capacity as i64),
            Frame::bulk("Size"),
            Frame::Integer(info.size as i64),
            Frame::bulk("Number of filters"),
            Frame::Integer(info.num_filters as i64),
            Frame::bulk("Number of items inserted"),
            Frame::Integer(info.num_items as i64),
            Frame::bulk("Expansion rate"),
            Frame::Integer(info.expansion_rate as i64),
        ]))
    })
}

/// BF.CARD key
/// Get the cardinality (number of items) in a Bloom filter.
pub fn cmd_bf_card(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;
        let key = cmd.args[0].to_vec();

        let filters = BLOOM_FILTERS.read();
        let count = filters.get(&key).map(|f| f.len()).unwrap_or(0);

        Ok(Frame::Integer(count as i64))
    })
}

/// BF.INSERT key [CAPACITY capacity] [ERROR error] [EXPANSION expansion] [NOCREATE] [NONSCALING] ITEMS item [item ...]
/// Insert items into a Bloom filter, creating it if necessary.
pub fn cmd_bf_insert(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();

        // Parse options
        let mut capacity = 1000usize;
        let mut error_rate = 0.01f64;
        let mut expansion = 2u32;
        let mut nocreate = false;
        let mut items_start = 1;

        let mut i = 1;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "CAPACITY" => {
                    i += 1;
                    capacity = cmd.get_u64(i)? as usize;
                }
                "ERROR" => {
                    i += 1;
                    error_rate = cmd.get_str(i)?.parse().map_err(|_| CommandError::NotFloat)?;
                }
                "EXPANSION" => {
                    i += 1;
                    expansion = cmd.get_u64(i)? as u32;
                }
                "NOCREATE" => {
                    nocreate = true;
                }
                "NONSCALING" => {
                    expansion = 1;
                }
                "ITEMS" => {
                    items_start = i + 1;
                    break;
                }
                _ => {}
            }
            i += 1;
        }

        if items_start >= cmd.args.len() {
            return Err(CommandError::WrongArity {
                command: "BF.INSERT".to_string(),
            }
            .into());
        }

        let mut filters = BLOOM_FILTERS.write();

        if nocreate && !filters.contains_key(&key) {
            return Err(CommandError::NoSuchKey.into());
        }

        let filter = filters
            .entry(key)
            .or_insert_with(|| ScalingBloomFilter::new(capacity, error_rate, expansion));

        let results: Vec<Frame> = cmd.args[items_start..]
            .iter()
            .map(|item| {
                let added = filter.add(item);
                Frame::Integer(if added { 1 } else { 0 })
            })
            .collect();

        Ok(Frame::Array(results))
    })
}
