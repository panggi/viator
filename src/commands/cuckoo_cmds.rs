//! Cuckoo filter command implementations.
//!
//! Redis Stack compatible CF.* commands.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::CuckooFilter;
use crate::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Global storage for cuckoo filters.
static CUCKOO_FILTERS: std::sync::LazyLock<RwLock<HashMap<Vec<u8>, CuckooFilter>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// CF.ADD key item
/// Add an item to a Cuckoo filter.
pub fn cmd_cf_add(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;
        let key = cmd.args[0].to_vec();
        let item = &cmd.args[1];

        let mut filters = CUCKOO_FILTERS.write();
        let filter = filters.entry(key).or_insert_with(|| CuckooFilter::new(1000));

        let added = filter.add(item);
        Ok(Frame::Integer(if added { 1 } else { 0 }))
    })
}

/// CF.ADDNX key item
/// Add an item to a Cuckoo filter only if it doesn't exist.
pub fn cmd_cf_addnx(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;
        let key = cmd.args[0].to_vec();
        let item = &cmd.args[1];

        let mut filters = CUCKOO_FILTERS.write();
        let filter = filters.entry(key).or_insert_with(|| CuckooFilter::new(1000));

        let added = filter.add_nx(item);
        Ok(Frame::Integer(if added { 1 } else { 0 }))
    })
}

/// CF.EXISTS key item
/// Check if an item exists in a Cuckoo filter.
pub fn cmd_cf_exists(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;
        let key = cmd.args[0].to_vec();
        let item = &cmd.args[1];

        let filters = CUCKOO_FILTERS.read();
        let exists = filters.get(&key).map(|f| f.contains(item)).unwrap_or(false);

        Ok(Frame::Integer(if exists { 1 } else { 0 }))
    })
}

/// CF.MEXISTS key item [item ...]
/// Check if multiple items exist in a Cuckoo filter.
pub fn cmd_cf_mexists(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let filters = CUCKOO_FILTERS.read();
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

/// CF.DEL key item
/// Delete an item from a Cuckoo filter.
pub fn cmd_cf_del(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;
        let key = cmd.args[0].to_vec();
        let item = &cmd.args[1];

        let mut filters = CUCKOO_FILTERS.write();
        let deleted = filters
            .get_mut(&key)
            .map(|f| f.remove(item))
            .unwrap_or(false);

        Ok(Frame::Integer(if deleted { 1 } else { 0 }))
    })
}

/// CF.COUNT key item
/// Get the count of an item in a Cuckoo filter.
pub fn cmd_cf_count(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;
        let key = cmd.args[0].to_vec();
        let item = &cmd.args[1];

        let filters = CUCKOO_FILTERS.read();
        let count = filters.get(&key).map(|f| f.count(item)).unwrap_or(0);

        Ok(Frame::Integer(count as i64))
    })
}

/// CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]
/// Reserve a Cuckoo filter with specific parameters.
pub fn cmd_cf_reserve(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();
        let capacity = cmd.get_u64(1)? as usize;

        // Parse optional arguments
        let mut bucket_size = None;
        let mut max_iterations = None;
        let mut expansion = None;

        let mut i = 2;
        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "BUCKETSIZE" => {
                    i += 1;
                    bucket_size = Some(cmd.get_u64(i)? as usize);
                }
                "MAXITERATIONS" => {
                    i += 1;
                    max_iterations = Some(cmd.get_u64(i)? as usize);
                }
                "EXPANSION" => {
                    i += 1;
                    expansion = Some(cmd.get_u64(i)? as u16);
                }
                _ => {}
            }
            i += 1;
        }

        let mut filters = CUCKOO_FILTERS.write();
        if filters.contains_key(&key) {
            return Err(CommandError::KeyExists.into());
        }

        let filter = CuckooFilter::reserve(capacity, bucket_size, max_iterations, expansion);
        filters.insert(key, filter);

        Ok(Frame::ok())
    })
}

/// CF.INFO key
/// Get information about a Cuckoo filter.
pub fn cmd_cf_info(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;
        let key = cmd.args[0].to_vec();

        let filters = CUCKOO_FILTERS.read();
        let filter = filters.get(&key).ok_or(CommandError::NoSuchKey)?;

        let info = filter.info();
        Ok(Frame::Array(vec![
            Frame::bulk("Size"),
            Frame::Integer(info.size as i64),
            Frame::bulk("Number of buckets"),
            Frame::Integer(info.num_buckets as i64),
            Frame::bulk("Number of filters"),
            Frame::Integer(info.num_filters as i64),
            Frame::bulk("Number of items inserted"),
            Frame::Integer(info.num_items as i64),
            Frame::bulk("Number of items deleted"),
            Frame::Integer(info.num_items_deleted as i64),
            Frame::bulk("Bucket size"),
            Frame::Integer(info.bucket_size as i64),
            Frame::bulk("Expansion rate"),
            Frame::Integer(info.expansion_rate as i64),
            Frame::bulk("Max iterations"),
            Frame::Integer(info.max_iterations as i64),
        ]))
    })
}

/// CF.INSERT key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
/// Insert items into a Cuckoo filter.
pub fn cmd_cf_insert(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();

        // Parse options
        let mut capacity = 1000usize;
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
                "NOCREATE" => {
                    nocreate = true;
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
                command: "CF.INSERT".to_string(),
            }
            .into());
        }

        let mut filters = CUCKOO_FILTERS.write();

        if nocreate && !filters.contains_key(&key) {
            return Err(CommandError::NoSuchKey.into());
        }

        let filter = filters
            .entry(key)
            .or_insert_with(|| CuckooFilter::new(capacity));

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

/// CF.INSERTNX key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
/// Insert items into a Cuckoo filter only if they don't exist.
pub fn cmd_cf_insertnx(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let key = cmd.args[0].to_vec();

        // Parse options
        let mut capacity = 1000usize;
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
                "NOCREATE" => {
                    nocreate = true;
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
                command: "CF.INSERTNX".to_string(),
            }
            .into());
        }

        let mut filters = CUCKOO_FILTERS.write();

        if nocreate && !filters.contains_key(&key) {
            return Err(CommandError::NoSuchKey.into());
        }

        let filter = filters
            .entry(key)
            .or_insert_with(|| CuckooFilter::new(capacity));

        let results: Vec<Frame> = cmd.args[items_start..]
            .iter()
            .map(|item| {
                let added = filter.add_nx(item);
                Frame::Integer(if added { 1 } else { 0 })
            })
            .collect();

        Ok(Frame::Array(results))
    })
}
