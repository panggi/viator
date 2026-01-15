//! Count-Min Sketch command implementations.
//!
//! Redis Stack compatible CMS.* commands.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::CountMinSketch;
use crate::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Global storage for Count-Min Sketches.
static CMS_SKETCHES: std::sync::LazyLock<RwLock<HashMap<Vec<u8>, CountMinSketch>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// CMS.INITBYDIM key width depth
/// Initialize a Count-Min Sketch with specified dimensions.
pub fn cmd_cms_initbydim(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;
        let key = cmd.args[0].to_vec();
        let width = cmd.get_u64(1)? as usize;
        let depth = cmd.get_u64(2)? as usize;

        let mut sketches = CMS_SKETCHES.write();
        if sketches.contains_key(&key) {
            return Err(CommandError::KeyExists.into());
        }

        sketches.insert(key, CountMinSketch::new(width, depth));
        Ok(Frame::ok())
    })
}

/// CMS.INITBYPROB key error probability
/// Initialize a Count-Min Sketch with error rate and probability.
pub fn cmd_cms_initbyprob(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(3)?;
        let key = cmd.args[0].to_vec();
        let error: f64 = cmd.get_str(1)?.parse().map_err(|_| CommandError::NotFloat)?;
        let probability: f64 = cmd.get_str(2)?.parse().map_err(|_| CommandError::NotFloat)?;

        let mut sketches = CMS_SKETCHES.write();
        if sketches.contains_key(&key) {
            return Err(CommandError::KeyExists.into());
        }

        sketches.insert(key, CountMinSketch::from_error_rate(error, probability));
        Ok(Frame::ok())
    })
}

/// CMS.INCRBY key item increment [item increment ...]
/// Increment item counts in a Count-Min Sketch.
pub fn cmd_cms_incrby(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        if (cmd.args.len() - 1) % 2 != 0 {
            return Err(CommandError::WrongArity {
                command: "CMS.INCRBY".to_string(),
            }
            .into());
        }

        let key = cmd.args[0].to_vec();

        let mut sketches = CMS_SKETCHES.write();
        let sketch = sketches
            .entry(key)
            .or_insert_with(|| CountMinSketch::new(2000, 7));

        let mut results = Vec::new();
        let mut i = 1;
        while i < cmd.args.len() {
            let item = &cmd.args[i];
            let increment = cmd.get_u64(i + 1)?;
            sketch.increment(item, increment);
            // Query the estimated count after incrementing
            let count = sketch.query(item);
            results.push(Frame::Integer(count as i64));
            i += 2;
        }

        Ok(Frame::Array(results))
    })
}

/// CMS.QUERY key item [item ...]
/// Query item counts in a Count-Min Sketch.
pub fn cmd_cms_query(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;
        let key = cmd.args[0].to_vec();

        let sketches = CMS_SKETCHES.read();
        let sketch = sketches.get(&key);

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|item| {
                let count = sketch.map(|s| s.query(item)).unwrap_or(0);
                Frame::Integer(count as i64)
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// CMS.MERGE destkey numkeys key [key ...] [WEIGHTS weight [weight ...]]
/// Merge multiple Count-Min Sketches.
pub fn cmd_cms_merge(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;
        let dest_key = cmd.args[0].to_vec();
        let num_keys = cmd.get_u64(1)? as usize;

        if cmd.args.len() < 2 + num_keys {
            return Err(CommandError::WrongArity {
                command: "CMS.MERGE".to_string(),
            }
            .into());
        }

        // Parse source keys
        let source_keys: Vec<Vec<u8>> = cmd.args[2..2 + num_keys]
            .iter()
            .map(|b| b.to_vec())
            .collect();

        // Parse optional weights (not fully implemented - would need weighted merge)
        let mut _weights: Vec<u64> = vec![1; num_keys];
        let mut i = 2 + num_keys;
        if i < cmd.args.len() && cmd.get_str(i)?.to_uppercase() == "WEIGHTS" {
            i += 1;
            for (j, weight) in _weights.iter_mut().enumerate() {
                if i + j < cmd.args.len() {
                    *weight = cmd.get_u64(i + j)?;
                }
            }
        }

        let mut sketches = CMS_SKETCHES.write();

        // Get dimensions from first source
        let (width, depth) = {
            let first = sketches
                .get(&source_keys[0])
                .ok_or(CommandError::NoSuchKey)?;
            (first.width(), first.depth())
        };

        // Create destination sketch if it doesn't exist
        if !sketches.contains_key(&dest_key) {
            sketches.insert(dest_key.clone(), CountMinSketch::new(width, depth));
        }

        // Merge sources (basic merge without weights)
        for key in &source_keys {
            if let Some(source) = sketches.get(key).cloned() {
                if let Some(dest) = sketches.get_mut(&dest_key) {
                    let _ = dest.merge(&source);
                }
            }
        }

        Ok(Frame::ok())
    })
}

/// CMS.INFO key
/// Get information about a Count-Min Sketch.
pub fn cmd_cms_info(
    cmd: ParsedCommand,
    _db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(1)?;
        let key = cmd.args[0].to_vec();

        let sketches = CMS_SKETCHES.read();
        let sketch = sketches.get(&key).ok_or(CommandError::NoSuchKey)?;

        let info = sketch.info();
        Ok(Frame::Array(vec![
            Frame::bulk("width"),
            Frame::Integer(info.width as i64),
            Frame::bulk("depth"),
            Frame::Integer(info.depth as i64),
            Frame::bulk("count"),
            Frame::Integer(info.count as i64),
        ]))
    })
}
