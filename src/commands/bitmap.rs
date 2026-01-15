//! Bitmap command implementations.
//!
//! Redis bitmaps are stored as strings but provide bit-level operations.

use super::ParsedCommand;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, ViatorValue, ValueType};
use crate::Result;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// SETBIT key offset value
pub fn cmd_setbit(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let offset = cmd.get_u64(1)? as usize;
        let value = cmd.get_u64(2)?;

        if value > 1 {
            return Err(CommandError::BitValueOutOfRange.into());
        }

        // Calculate byte and bit position
        let byte_offset = offset / 8;
        let bit_offset = 7 - (offset % 8); // MSB first within byte

        // Get or create string value
        let current = db.get(&key);
        let mut data = match current {
            Some(v) if v.is_string() => v.as_string().unwrap().to_vec(),
            Some(_) => return Err(CommandError::WrongType.into()),
            None => Vec::new(),
        };

        // Extend if needed
        if data.len() <= byte_offset {
            data.resize(byte_offset + 1, 0);
        }

        // Get old bit value
        let old_value = (data[byte_offset] >> bit_offset) & 1;

        // Set new bit value
        if value == 1 {
            data[byte_offset] |= 1 << bit_offset;
        } else {
            data[byte_offset] &= !(1 << bit_offset);
        }

        db.set(key, ViatorValue::String(Bytes::from(data)));
        Ok(Frame::Integer(old_value as i64))
    })
}

/// GETBIT key offset
pub fn cmd_getbit(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let offset = cmd.get_u64(1)? as usize;

        let byte_offset = offset / 8;
        let bit_offset = 7 - (offset % 8);

        let value = match db.get_typed(&key, ValueType::String)? {
            Some(v) => {
                let data = v.as_string().unwrap();
                if byte_offset >= data.len() {
                    0
                } else {
                    (data[byte_offset] >> bit_offset) & 1
                }
            }
            None => 0,
        };

        Ok(Frame::Integer(value as i64))
    })
}

/// BITCOUNT key [start end [BYTE|BIT]]
pub fn cmd_bitcount(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());

        let value = match db.get_typed(&key, ValueType::String)? {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let data = value.as_string().unwrap();
        let len = data.len();

        if len == 0 {
            return Ok(Frame::Integer(0));
        }

        // Parse optional range
        let (start, end, bit_mode) = if cmd.args.len() >= 3 {
            let start = cmd.get_i64(1)?;
            let end = cmd.get_i64(2)?;

            let bit_mode = if cmd.args.len() >= 4 {
                let mode = cmd.get_str(3)?.to_uppercase();
                mode == "BIT"
            } else {
                false
            };

            (start, end, bit_mode)
        } else {
            (0i64, -1i64, false)
        };

        let count = if bit_mode {
            // Bit mode - count bits in specified bit range
            let total_bits = len * 8;
            let start = normalize_index(start, total_bits as i64) as usize;
            let end = normalize_index(end, total_bits as i64) as usize;

            if start > end || start >= total_bits {
                0
            } else {
                let end = std::cmp::min(end, total_bits - 1);
                count_bits_in_range(data, start, end)
            }
        } else {
            // Byte mode - count bits in specified byte range
            let start = normalize_index(start, len as i64) as usize;
            let end = normalize_index(end, len as i64) as usize;

            if start > end || start >= len {
                0
            } else {
                let end = std::cmp::min(end, len - 1);
                data[start..=end]
                    .iter()
                    .map(|b| b.count_ones() as usize)
                    .sum()
            }
        };

        Ok(Frame::Integer(count as i64))
    })
}

/// BITOP operation destkey key [key ...]
/// Supports: AND, OR, XOR, NOT, DIFF, DIFF1, ANDOR, ONE (Redis 8.2+)
pub fn cmd_bitop(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let operation = cmd.get_str(0)?.to_uppercase();
        let destkey = Key::from(cmd.args[1].clone());
        let keys: Vec<Key> = cmd.args[2..].iter().map(|b| Key::from(b.clone())).collect();

        if keys.is_empty() {
            return Err(CommandError::WrongArity {
                command: "BITOP".to_string(),
            }
            .into());
        }

        // Collect all source strings
        let mut strings: Vec<Vec<u8>> = Vec::new();
        let mut max_len = 0;

        for key in &keys {
            let data = match db.get_typed(key, ValueType::String)? {
                Some(v) => v.as_string().unwrap().to_vec(),
                None => Vec::new(),
            };
            max_len = std::cmp::max(max_len, data.len());
            strings.push(data);
        }

        // Handle NOT specially (unary operation)
        if operation == "NOT" {
            if strings.len() != 1 {
                return Err(CommandError::WrongArity {
                    command: "BITOP NOT".to_string(),
                }
                .into());
            }

            let mut result = strings[0].clone();
            result.resize(max_len, 0);
            for byte in &mut result {
                *byte = !*byte;
            }

            let result_len = result.len();
            db.set(destkey, ViatorValue::String(Bytes::from(result)));
            return Ok(Frame::Integer(result_len as i64));
        }

        // Binary operations
        let mut result = vec![0u8; max_len];

        match operation.as_str() {
            "AND" => {
                // Initialize with all 1s for AND
                result.fill(0xFF);
                for s in &strings {
                    for (i, byte) in result.iter_mut().enumerate() {
                        *byte &= s.get(i).copied().unwrap_or(0);
                    }
                }
            }
            "OR" => {
                for s in &strings {
                    for (i, byte) in result.iter_mut().enumerate() {
                        *byte |= s.get(i).copied().unwrap_or(0);
                    }
                }
            }
            "XOR" => {
                for s in &strings {
                    for (i, byte) in result.iter_mut().enumerate() {
                        *byte ^= s.get(i).copied().unwrap_or(0);
                    }
                }
            }
            // Redis 8.2+ new operators
            "DIFF" => {
                // A AND NOT B (difference: bits in first but not in second)
                // For multiple keys: A AND NOT B AND NOT C ...
                if strings.is_empty() {
                    return Err(CommandError::WrongArity {
                        command: "BITOP DIFF".to_string(),
                    }
                    .into());
                }
                // Start with first string
                for (i, byte) in result.iter_mut().enumerate() {
                    *byte = strings[0].get(i).copied().unwrap_or(0);
                }
                // AND NOT with each subsequent string
                for s in strings.iter().skip(1) {
                    for (i, byte) in result.iter_mut().enumerate() {
                        *byte &= !s.get(i).copied().unwrap_or(0);
                    }
                }
            }
            "DIFF1" => {
                // First key minus union of all others: A AND NOT (B OR C OR ...)
                if strings.len() < 2 {
                    return Err(CommandError::WrongArity {
                        command: "BITOP DIFF1".to_string(),
                    }
                    .into());
                }
                // First compute union of all strings except first
                let mut union = vec![0u8; max_len];
                for s in strings.iter().skip(1) {
                    for (i, byte) in union.iter_mut().enumerate() {
                        *byte |= s.get(i).copied().unwrap_or(0);
                    }
                }
                // Then compute first AND NOT union
                for (i, byte) in result.iter_mut().enumerate() {
                    *byte = strings[0].get(i).copied().unwrap_or(0) & !union[i];
                }
            }
            "ANDOR" => {
                // (A AND B) OR (C AND D) OR ... - requires pairs
                if strings.len() % 2 != 0 {
                    return Err(CommandError::WrongArity {
                        command: "BITOP ANDOR".to_string(),
                    }
                    .into());
                }
                // Process pairs
                for pair in strings.chunks(2) {
                    for i in 0..max_len {
                        let a = pair[0].get(i).copied().unwrap_or(0);
                        let b = pair[1].get(i).copied().unwrap_or(0);
                        result[i] |= a & b;
                    }
                }
            }
            "ONE" => {
                // Set bit to 1 if exactly one of the inputs has it set
                // Count per bit position across all strings
                for i in 0..max_len {
                    let mut result_byte = 0u8;
                    for bit in 0..8 {
                        let mut count = 0;
                        for s in &strings {
                            let byte = s.get(i).copied().unwrap_or(0);
                            if (byte >> bit) & 1 == 1 {
                                count += 1;
                            }
                        }
                        if count == 1 {
                            result_byte |= 1 << bit;
                        }
                    }
                    result[i] = result_byte;
                }
            }
            _ => {
                return Err(CommandError::SyntaxError.into());
            }
        }

        let result_len = result.len();
        db.set(destkey, ViatorValue::String(Bytes::from(result)));
        Ok(Frame::Integer(result_len as i64))
    })
}

/// BITPOS key bit [start [end [BYTE|BIT]]]
pub fn cmd_bitpos(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let key = Key::from(cmd.args[0].clone());
        let target_bit = cmd.get_u64(1)?;

        if target_bit > 1 {
            return Err(CommandError::BitValueOutOfRange.into());
        }

        let value = match db.get_typed(&key, ValueType::String)? {
            Some(v) => v,
            None => {
                // Empty string: looking for 0 returns 0, looking for 1 returns -1
                return Ok(Frame::Integer(if target_bit == 0 { 0 } else { -1 }));
            }
        };

        let data = value.as_string().unwrap();
        let len = data.len();

        if len == 0 {
            return Ok(Frame::Integer(if target_bit == 0 { 0 } else { -1 }));
        }

        // Parse optional range
        let (start, end, bit_mode, has_end) = if cmd.args.len() >= 3 {
            let start = cmd.get_i64(2)?;
            let (end, has_end) = if cmd.args.len() >= 4 {
                (cmd.get_i64(3)?, true)
            } else {
                (-1i64, false)
            };

            let bit_mode = if cmd.args.len() >= 5 {
                let mode = cmd.get_str(4)?.to_uppercase();
                mode == "BIT"
            } else {
                false
            };

            (start, end, bit_mode, has_end)
        } else {
            (0i64, -1i64, false, false)
        };

        if bit_mode {
            // Bit mode
            let total_bits = len * 8;
            let start = normalize_index(start, total_bits as i64) as usize;
            let end = if has_end {
                normalize_index(end, total_bits as i64) as usize
            } else {
                total_bits - 1
            };

            if start > end || start >= total_bits {
                return Ok(Frame::Integer(-1));
            }

            for bit_pos in start..=std::cmp::min(end, total_bits - 1) {
                let byte_idx = bit_pos / 8;
                let bit_idx = 7 - (bit_pos % 8);
                let bit_val = (data[byte_idx] >> bit_idx) & 1;

                if bit_val == target_bit as u8 {
                    return Ok(Frame::Integer(bit_pos as i64));
                }
            }

            Ok(Frame::Integer(-1))
        } else {
            // Byte mode
            let start = normalize_index(start, len as i64) as usize;
            let end = if has_end {
                normalize_index(end, len as i64) as usize
            } else {
                len - 1
            };

            if start > end || start >= len {
                return Ok(Frame::Integer(-1));
            }

            let end = std::cmp::min(end, len - 1);

            for byte_idx in start..=end {
                let byte = data[byte_idx];
                for bit_idx in 0..8 {
                    let bit_val = (byte >> (7 - bit_idx)) & 1;
                    if bit_val == target_bit as u8 {
                        return Ok(Frame::Integer((byte_idx * 8 + bit_idx) as i64));
                    }
                }
            }

            // If looking for 0 and not found within range, check if range was explicit
            if target_bit == 0 && !has_end {
                // Return position just after the string
                return Ok(Frame::Integer((len * 8) as i64));
            }

            Ok(Frame::Integer(-1))
        }
    })
}

/// GETEX key - get the value and optionally set expiration (already exists in strings)
/// For bitmaps, we just need SETEX equivalent

/// STRLEN - returns string length (already exists in strings)

/// Helper to normalize negative indices
fn normalize_index(index: i64, len: i64) -> i64 {
    if index < 0 {
        std::cmp::max(0, len + index)
    } else {
        index
    }
}

/// Helper to count bits in a bit range
fn count_bits_in_range(data: &Bytes, start_bit: usize, end_bit: usize) -> usize {
    let mut count = 0;

    for bit_pos in start_bit..=end_bit {
        let byte_idx = bit_pos / 8;
        let bit_idx = 7 - (bit_pos % 8);

        if byte_idx < data.len() {
            count += ((data[byte_idx] >> bit_idx) & 1) as usize;
        }
    }

    count
}

/// Overflow behavior for BITFIELD operations
#[derive(Clone, Copy, Debug)]
enum OverflowBehavior {
    Wrap,
    Sat,
    Fail,
}

/// Parse integer type specification (e.g., "u8", "i16")
fn parse_int_type(s: &str) -> Option<(bool, u8)> {
    let s = s.to_lowercase();
    if s.len() < 2 {
        return None;
    }

    let signed = s.starts_with('i');
    if !signed && !s.starts_with('u') {
        return None;
    }

    let bits: u8 = s[1..].parse().ok()?;

    // Valid range: u1-u63, i1-i64
    if bits == 0 {
        return None;
    }
    if signed && bits > 64 {
        return None;
    }
    if !signed && bits > 63 {
        return None;
    }

    Some((signed, bits))
}

/// Parse offset (can be prefixed with # for bit offset multiplication)
fn parse_bitfield_offset(s: &str, bits: u8) -> Option<usize> {
    if let Some(rest) = s.strip_prefix('#') {
        let multiplier: usize = rest.parse().ok()?;
        Some(multiplier * bits as usize)
    } else {
        s.parse().ok()
    }
}

/// Get integer value from bitmap at specified bit offset
fn get_int_from_bits(data: &[u8], offset: usize, bits: u8, signed: bool) -> i64 {
    let mut value: u64 = 0;

    for i in 0..bits as usize {
        let bit_pos = offset + i;
        let byte_idx = bit_pos / 8;
        let bit_idx = 7 - (bit_pos % 8);

        if byte_idx < data.len() {
            let bit = ((data[byte_idx] >> bit_idx) & 1) as u64;
            value = (value << 1) | bit;
        } else {
            value <<= 1;
        }
    }

    if signed && bits < 64 {
        // Sign extend if needed
        let sign_bit = 1u64 << (bits - 1);
        if value & sign_bit != 0 {
            // Negative number - extend sign
            let mask = !((1u64 << bits) - 1);
            value |= mask;
        }
    }

    value as i64
}

/// Set integer value in bitmap at specified bit offset
fn set_int_to_bits(data: &mut Vec<u8>, offset: usize, bits: u8, value: i64) {
    let value = value as u64;

    // Ensure data is large enough
    let end_byte = (offset + bits as usize + 7) / 8;
    if data.len() < end_byte {
        data.resize(end_byte, 0);
    }

    for i in 0..bits as usize {
        let bit_pos = offset + i;
        let byte_idx = bit_pos / 8;
        let bit_idx = 7 - (bit_pos % 8);

        // Get the bit from value (MSB first)
        let bit_in_value = bits as usize - 1 - i;
        let bit = ((value >> bit_in_value) & 1) as u8;

        if bit == 1 {
            data[byte_idx] |= 1 << bit_idx;
        } else {
            data[byte_idx] &= !(1 << bit_idx);
        }
    }
}

/// Apply overflow handling
fn apply_overflow(value: i64, bits: u8, signed: bool, behavior: OverflowBehavior) -> Option<i64> {
    let (min, max) = if signed {
        let max = (1i64 << (bits - 1)) - 1;
        let min = -(1i64 << (bits - 1));
        (min, max)
    } else {
        let max = (1i64 << bits) - 1;
        (0i64, max)
    };

    match behavior {
        OverflowBehavior::Wrap => {
            if signed {
                // Wrap for signed
                let range = 1i64 << bits;
                let mut result = value % range;
                if result > max {
                    result -= range;
                } else if result < min {
                    result += range;
                }
                Some(result)
            } else {
                // Wrap for unsigned
                let mask = (1u64 << bits) - 1;
                Some((value as u64 & mask) as i64)
            }
        }
        OverflowBehavior::Sat => {
            Some(value.clamp(min, max))
        }
        OverflowBehavior::Fail => {
            if value < min || value > max {
                None
            } else {
                Some(value)
            }
        }
    }
}

/// BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]
pub fn cmd_bitfield(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        // Get current value
        let current = db.get(&key);
        let mut data = match current {
            Some(v) if v.is_string() => v.as_string().unwrap().to_vec(),
            Some(_) => return Err(CommandError::WrongType.into()),
            None => Vec::new(),
        };

        let mut results: Vec<Frame> = Vec::new();
        let mut overflow = OverflowBehavior::Wrap;
        let mut modified = false;

        let mut i = 1;
        while i < cmd.args.len() {
            let op = cmd.get_str(i)?.to_uppercase();

            match op.as_str() {
                "GET" => {
                    if i + 2 >= cmd.args.len() {
                        return Err(CommandError::SyntaxError.into());
                    }

                    let type_str = cmd.get_str(i + 1)?;
                    let offset_str = cmd.get_str(i + 2)?;

                    let (signed, bits) = parse_int_type(type_str)
                        .ok_or(CommandError::SyntaxError)?;
                    let offset = parse_bitfield_offset(offset_str, bits)
                        .ok_or(CommandError::SyntaxError)?;

                    let value = get_int_from_bits(&data, offset, bits, signed);
                    results.push(Frame::Integer(value));

                    i += 3;
                }
                "SET" => {
                    if i + 3 >= cmd.args.len() {
                        return Err(CommandError::SyntaxError.into());
                    }

                    let type_str = cmd.get_str(i + 1)?;
                    let offset_str = cmd.get_str(i + 2)?;
                    let value = cmd.get_i64(i + 3)?;

                    let (signed, bits) = parse_int_type(type_str)
                        .ok_or(CommandError::SyntaxError)?;
                    let offset = parse_bitfield_offset(offset_str, bits)
                        .ok_or(CommandError::SyntaxError)?;

                    // Get old value first
                    let old_value = get_int_from_bits(&data, offset, bits, signed);

                    // Apply overflow and set
                    if let Some(new_value) = apply_overflow(value, bits, signed, overflow) {
                        set_int_to_bits(&mut data, offset, bits, new_value);
                        modified = true;
                    }

                    results.push(Frame::Integer(old_value));
                    i += 4;
                }
                "INCRBY" => {
                    if i + 3 >= cmd.args.len() {
                        return Err(CommandError::SyntaxError.into());
                    }

                    let type_str = cmd.get_str(i + 1)?;
                    let offset_str = cmd.get_str(i + 2)?;
                    let increment = cmd.get_i64(i + 3)?;

                    let (signed, bits) = parse_int_type(type_str)
                        .ok_or(CommandError::SyntaxError)?;
                    let offset = parse_bitfield_offset(offset_str, bits)
                        .ok_or(CommandError::SyntaxError)?;

                    let old_value = get_int_from_bits(&data, offset, bits, signed);
                    let new_value = old_value.wrapping_add(increment);

                    if let Some(final_value) = apply_overflow(new_value, bits, signed, overflow) {
                        set_int_to_bits(&mut data, offset, bits, final_value);
                        modified = true;
                        results.push(Frame::Integer(final_value));
                    } else {
                        results.push(Frame::Null);
                    }

                    i += 4;
                }
                "OVERFLOW" => {
                    if i + 1 >= cmd.args.len() {
                        return Err(CommandError::SyntaxError.into());
                    }

                    let behavior = cmd.get_str(i + 1)?.to_uppercase();
                    overflow = match behavior.as_str() {
                        "WRAP" => OverflowBehavior::Wrap,
                        "SAT" => OverflowBehavior::Sat,
                        "FAIL" => OverflowBehavior::Fail,
                        _ => return Err(CommandError::SyntaxError.into()),
                    };

                    i += 2;
                }
                _ => return Err(CommandError::SyntaxError.into()),
            }
        }

        if modified {
            db.set(key, ViatorValue::String(Bytes::from(data)));
        }

        Ok(Frame::Array(results))
    })
}

/// BITFIELD_RO key [GET type offset ...]
/// Read-only variant of BITFIELD (only supports GET operations)
pub fn cmd_bitfield_ro(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let key = Key::from(cmd.args[0].clone());

        let data = match db.get_typed(&key, ValueType::String)? {
            Some(v) => v.as_string().unwrap().to_vec(),
            None => Vec::new(),
        };

        let mut results: Vec<Frame> = Vec::new();

        let mut i = 1;
        while i < cmd.args.len() {
            let op = cmd.get_str(i)?.to_uppercase();

            match op.as_str() {
                "GET" => {
                    if i + 2 >= cmd.args.len() {
                        return Err(CommandError::SyntaxError.into());
                    }

                    let type_str = cmd.get_str(i + 1)?;
                    let offset_str = cmd.get_str(i + 2)?;

                    let (signed, bits) = parse_int_type(type_str)
                        .ok_or(CommandError::SyntaxError)?;
                    let offset = parse_bitfield_offset(offset_str, bits)
                        .ok_or(CommandError::SyntaxError)?;

                    let value = get_int_from_bits(&data, offset, bits, signed);
                    results.push(Frame::Integer(value));

                    i += 3;
                }
                _ => return Err(CommandError::SyntaxError.into()),
            }
        }

        Ok(Frame::Array(results))
    })
}
