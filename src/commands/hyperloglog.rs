//! HyperLogLog command implementations.
//!
//! HyperLogLog is a probabilistic data structure used for cardinality estimation.
//! It can estimate the number of unique elements in a set with a standard error
//! of approximately 0.81%.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, ValueType, ViatorValue};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Number of registers in the HyperLogLog (2^14 = 16384)
const HLL_REGISTERS: usize = 16384;

/// Alpha constant for bias correction (for m = 16384)
const HLL_ALPHA: f64 = 0.7213 / (1.0 + 1.079 / HLL_REGISTERS as f64);

/// Magic header for HyperLogLog
const HLL_MAGIC: &[u8] = b"HYLL";

/// HyperLogLog data structure
#[derive(Clone)]
struct HyperLogLog {
    registers: Vec<u8>,
}

impl HyperLogLog {
    /// Create a new HyperLogLog
    fn new() -> Self {
        Self {
            registers: vec![0; HLL_REGISTERS],
        }
    }

    /// Create from raw bytes (Redis format)
    fn from_bytes(data: &[u8]) -> Option<Self> {
        // Check magic header
        if data.len() < 4 || &data[0..4] != HLL_MAGIC {
            // Try to parse as raw register data
            if data.len() == HLL_REGISTERS {
                return Some(Self {
                    registers: data.to_vec(),
                });
            }
            return None;
        }

        // Skip header (HYLL + metadata)
        // For simplicity, we'll use a minimal header format
        if data.len() >= 4 + HLL_REGISTERS {
            Some(Self {
                registers: data[4..4 + HLL_REGISTERS].to_vec(),
            })
        } else {
            None
        }
    }

    /// Convert to bytes for storage
    fn to_bytes(&self) -> Bytes {
        let mut data = Vec::with_capacity(4 + HLL_REGISTERS);
        data.extend_from_slice(HLL_MAGIC);
        data.extend_from_slice(&self.registers);
        Bytes::from(data)
    }

    /// Add an element to the HyperLogLog
    fn add(&mut self, element: &[u8]) -> bool {
        let hash = self.hash(element);

        // First 14 bits determine the register
        let index = (hash & 0x3FFF) as usize;

        // Remaining bits are used to count leading zeros
        let w = hash >> 14;
        let count = self.count_leading_zeros(w) + 1;

        // Update register if new count is larger
        if count > self.registers[index] {
            self.registers[index] = count;
            true
        } else {
            false
        }
    }

    /// Count the number of leading zeros in a 50-bit value
    fn count_leading_zeros(&self, mut value: u64) -> u8 {
        // We only use 50 bits (64 - 14)
        if value == 0 {
            return 50;
        }

        let mut count = 0u8;
        // Check the 50-bit window
        for _ in 0..50 {
            if value & (1u64 << 49) == 0 {
                count += 1;
                value <<= 1;
            } else {
                break;
            }
        }
        count
    }

    /// Simple hash function (MurmurHash-like)
    fn hash(&self, data: &[u8]) -> u64 {
        // Simple but effective hash for HLL purposes
        let mut h: u64 = 0xcbf29ce484222325; // FNV offset basis
        for &byte in data {
            h ^= byte as u64;
            h = h.wrapping_mul(0x100000001b3); // FNV prime
        }
        // Mix the bits
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        h
    }

    /// Estimate the cardinality
    fn count(&self) -> u64 {
        // Calculate raw estimate using harmonic mean
        let mut sum = 0.0;
        let mut zeros = 0;

        for &reg in &self.registers {
            sum += 2.0_f64.powi(-(reg as i32));
            if reg == 0 {
                zeros += 1;
            }
        }

        let estimate = HLL_ALPHA * (HLL_REGISTERS as f64).powi(2) / sum;

        // Apply small range correction
        if estimate <= 2.5 * HLL_REGISTERS as f64 && zeros > 0 {
            // Linear counting for small cardinalities
            let linear = (HLL_REGISTERS as f64) * (HLL_REGISTERS as f64 / zeros as f64).ln();
            return linear as u64;
        }

        // Apply large range correction
        let two_to_32: f64 = 4294967296.0; // 2^32
        if estimate > two_to_32 / 30.0 {
            return (-two_to_32 * (1.0 - estimate / two_to_32).ln()) as u64;
        }

        estimate as u64
    }

    /// Merge another HyperLogLog into this one
    fn merge(&mut self, other: &HyperLogLog) {
        for (i, &other_val) in other.registers.iter().enumerate() {
            if other_val > self.registers[i] {
                self.registers[i] = other_val;
            }
        }
    }
}

/// Get or create a HyperLogLog from the database
fn get_or_create_hll(db: &Db, key: &Key) -> Result<(ViatorValue, HyperLogLog)> {
    match db.get(key) {
        Some(v) if v.is_string() => {
            let data = v.as_string().expect("type verified before access");
            let hll = HyperLogLog::from_bytes(data).unwrap_or_else(HyperLogLog::new);
            Ok((v, hll))
        }
        Some(_) => Err(CommandError::WrongType.into()),
        None => {
            let hll = HyperLogLog::new();
            let value = ViatorValue::String(hll.to_bytes());
            Ok((value, hll))
        }
    }
}

/// PFADD key element [element ...]
pub fn cmd_pfadd(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let (_, mut hll) = get_or_create_hll(&db, &key)?;

        let mut modified = false;
        for element in &cmd.args[1..] {
            if hll.add(element) {
                modified = true;
            }
        }

        // Save the HLL
        db.set(key, ViatorValue::String(hll.to_bytes()));

        Ok(Frame::Integer(if modified { 1 } else { 0 }))
    })
}

/// PFCOUNT key [key ...]
pub fn cmd_pfcount(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.len() == 1 {
            // Single key
            let key = Key::from(cmd.args[0].clone());
            let count = match db.get_typed(&key, ValueType::String)? {
                Some(v) => {
                    let data = v.as_string().expect("type verified before access");
                    let hll = HyperLogLog::from_bytes(data).unwrap_or_else(HyperLogLog::new);
                    hll.count()
                }
                None => 0,
            };
            Ok(Frame::Integer(count as i64))
        } else {
            // Multiple keys - merge and count
            let mut merged = HyperLogLog::new();

            for arg in &cmd.args {
                let key = Key::from(arg.clone());
                if let Some(v) = db.get_typed(&key, ValueType::String)? {
                    let data = v.as_string().expect("type verified before access");
                    if let Some(hll) = HyperLogLog::from_bytes(data) {
                        merged.merge(&hll);
                    }
                }
            }

            Ok(Frame::Integer(merged.count() as i64))
        }
    })
}

/// PFMERGE destkey sourcekey [sourcekey ...]
pub fn cmd_pfmerge(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let destkey = Key::from(cmd.args[0].clone());
        let mut merged = HyperLogLog::new();

        // Merge all source keys
        for arg in &cmd.args[1..] {
            let key = Key::from(arg.clone());
            if let Some(v) = db.get_typed(&key, ValueType::String)? {
                let data = v.as_string().expect("type verified before access");
                if let Some(hll) = HyperLogLog::from_bytes(data) {
                    merged.merge(&hll);
                }
            }
        }

        // Also include the destination key if it exists
        if let Some(v) = db.get_typed(&destkey, ValueType::String)? {
            let data = v.as_string().expect("type verified before access");
            if let Some(hll) = HyperLogLog::from_bytes(data) {
                merged.merge(&hll);
            }
        }

        // Save the merged result
        db.set(destkey, ViatorValue::String(merged.to_bytes()));

        Ok(Frame::ok())
    })
}

/// PFDEBUG subcommand key [elements...]
/// This is for debugging/testing HLL internals
pub fn cmd_pfdebug(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        if cmd.args.is_empty() {
            return Err(CommandError::WrongArity {
                command: "PFDEBUG".to_string(),
            }
            .into());
        }

        let subcommand = cmd.get_str(0)?.to_uppercase();

        match subcommand.as_str() {
            "GETREG" => {
                // Get register values
                if cmd.args.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "PFDEBUG GETREG".to_string(),
                    }
                    .into());
                }
                let key = Key::from(cmd.args[1].clone());
                let hll = match db.get_typed(&key, ValueType::String)? {
                    Some(v) => {
                        let data = v.as_string().expect("type verified before access");
                        HyperLogLog::from_bytes(data).unwrap_or_else(HyperLogLog::new)
                    }
                    None => HyperLogLog::new(),
                };

                let frames: Vec<Frame> = hll
                    .registers
                    .iter()
                    .map(|&r| Frame::Integer(r as i64))
                    .collect();
                Ok(Frame::Array(frames))
            }
            _ => Err(CommandError::SyntaxError.into()),
        }
    })
}
