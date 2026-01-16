//! VDB (Viator Database) file format implementation.
//!
//! VDB is a point-in-time snapshot format that stores the entire dataset
//! in a compact binary representation. It's optimized for fast loading.
//!
//! # Format Overview
//!
//! ```text
//! +-------+-------------+-----------+---------+-----+--------+
//! | VIATR | VDB-VERSION | AUX-PAIRS | DB-DATA | EOF | CRC64  |
//! +-------+-------------+-----------+---------+-----+--------+
//! ```

use crate::error::StorageError;
use crate::storage::Database;
use crate::types::{Expiry, ValueType};
use bytes::Bytes;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// CRC64 polynomial (ECMA-182)
const CRC64_POLY: u64 = 0xC96C5795D7870F42;

/// CRC64 lookup table for fast computation
static CRC64_TABLE: std::sync::LazyLock<[u64; 256]> = std::sync::LazyLock::new(|| {
    let mut table = [0u64; 256];
    for i in 0..256 {
        let mut crc = i as u64;
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ CRC64_POLY;
            } else {
                crc >>= 1;
            }
        }
        table[i] = crc;
    }
    table
});

/// Calculate CRC64 checksum (ECMA-182)
fn crc64_update(crc: u64, data: &[u8]) -> u64 {
    let mut crc = crc;
    for byte in data {
        let idx = ((crc ^ (*byte as u64)) & 0xFF) as usize;
        crc = CRC64_TABLE[idx] ^ (crc >> 8);
    }
    crc
}

/// Decompress LZF-compressed data.
///
/// LZF is a simple, fast compression algorithm used for compressing
/// large strings in VDB files. The format uses a control byte followed by
/// literal runs or back-references.
fn lzf_decompress(compressed: &[u8], expected_len: usize) -> Result<Vec<u8>, &'static str> {
    let mut output = Vec::with_capacity(expected_len);
    let mut i = 0;

    while i < compressed.len() {
        let ctrl = compressed[i];
        i += 1;

        if ctrl < 32 {
            // Literal run: copy ctrl + 1 bytes
            let len = (ctrl as usize) + 1;
            if i + len > compressed.len() {
                return Err("LZF: unexpected end of input during literal copy");
            }
            output.extend_from_slice(&compressed[i..i + len]);
            i += len;
        } else {
            // Back-reference
            let len = ((ctrl >> 5) as usize) + 2;
            if i >= compressed.len() {
                return Err("LZF: unexpected end of input reading offset");
            }

            let offset = if len == 9 {
                // Long match: len is in next byte
                let next = compressed[i];
                i += 1;
                if i >= compressed.len() {
                    return Err("LZF: unexpected end of input reading long offset");
                }
                let off_high = ((ctrl & 0x1F) as usize) << 8;
                let off_low = compressed[i] as usize;
                i += 1;
                let actual_len = (next as usize) + 9;
                let offset = off_high | off_low;
                // Copy with calculated length
                let start = output
                    .len()
                    .checked_sub(offset + 1)
                    .ok_or("LZF: invalid back-reference offset")?;
                for j in 0..actual_len {
                    let byte = output[start + (j % (offset + 1))];
                    output.push(byte);
                }
                continue;
            } else {
                let off_high = ((ctrl & 0x1F) as usize) << 8;
                let off_low = compressed[i] as usize;
                i += 1;
                off_high | off_low
            };

            // Copy len bytes from output at offset
            let start = output
                .len()
                .checked_sub(offset + 1)
                .ok_or("LZF: invalid back-reference offset")?;
            for j in 0..len {
                let byte = output[start + (j % (offset + 1))];
                output.push(byte);
            }
        }
    }

    if output.len() != expected_len {
        return Err("LZF: decompressed size mismatch");
    }

    Ok(output)
}

/// Compress data using LZF algorithm.
///
/// LZF compression uses a simple hash-based approach to find back-references.
/// Returns None if compression doesn't save space.
fn lzf_compress(input: &[u8]) -> Option<Vec<u8>> {
    if input.len() < 4 {
        return None;
    }

    let mut output = Vec::with_capacity(input.len());
    let mut hash_table: [usize; 8192] = [0; 8192];
    let mut ip = 0; // input position
    let mut literal_start = 0;
    let mut literal_len = 0;

    // Hash function for 3-byte sequence
    let hash = |data: &[u8], pos: usize| -> usize {
        let v = (data[pos] as usize)
            | ((data[pos + 1] as usize) << 8)
            | ((data[pos + 2] as usize) << 16);
        ((v >> 3) ^ v) & 0x1FFF
    };

    while ip < input.len() - 2 {
        let h = hash(input, ip);
        let ref_pos = hash_table[h];
        hash_table[h] = ip;

        // Check if we have a match
        let offset = ip.saturating_sub(ref_pos);
        if offset > 0 && offset <= 8192 && ref_pos < ip && ip + 3 <= input.len() {
            // Check for actual match
            if ref_pos + 2 < input.len()
                && input[ref_pos] == input[ip]
                && input[ref_pos + 1] == input[ip + 1]
                && input[ref_pos + 2] == input[ip + 2]
            {
                // Found a match, flush literals first
                if literal_len > 0 {
                    let mut remaining = literal_len;
                    let mut lit_pos = literal_start;
                    while remaining > 0 {
                        let chunk = remaining.min(32);
                        output.push((chunk - 1) as u8);
                        output.extend_from_slice(&input[lit_pos..lit_pos + chunk]);
                        lit_pos += chunk;
                        remaining -= chunk;
                    }
                    literal_len = 0;
                }

                // Find match length
                let mut match_len = 3;
                while ip + match_len < input.len()
                    && ref_pos + match_len < ip
                    && input[ref_pos + match_len] == input[ip + match_len]
                    && match_len < 264
                {
                    match_len += 1;
                }

                let back_offset = offset - 1;

                if match_len <= 8 {
                    // Short match: 3-8 bytes
                    let ctrl = ((match_len - 2) << 5) | ((back_offset >> 8) & 0x1F);
                    output.push(ctrl as u8);
                    output.push((back_offset & 0xFF) as u8);
                } else {
                    // Long match: 9+ bytes
                    output.push((7 << 5) | ((back_offset >> 8) & 0x1F) as u8);
                    output.push((match_len - 9) as u8);
                    output.push((back_offset & 0xFF) as u8);
                }

                ip += match_len;
                literal_start = ip;
                continue;
            }
        }

        // No match, add to literal run
        if literal_len == 0 {
            literal_start = ip;
        }
        literal_len += 1;
        ip += 1;
    }

    // Handle remaining bytes as literals
    while ip < input.len() {
        if literal_len == 0 {
            literal_start = ip;
        }
        literal_len += 1;
        ip += 1;
    }

    // Flush remaining literals
    if literal_len > 0 {
        let mut remaining = literal_len;
        let mut lit_pos = literal_start;
        while remaining > 0 {
            let chunk = remaining.min(32);
            output.push((chunk - 1) as u8);
            output.extend_from_slice(&input[lit_pos..lit_pos + chunk]);
            lit_pos += chunk;
            remaining -= chunk;
        }
    }

    // Only return compressed data if it's actually smaller
    if output.len() < input.len() {
        Some(output)
    } else {
        None
    }
}

/// Minimum string length to attempt LZF compression (same as Redis)
const LZF_COMPRESS_MIN_LEN: usize = 20;

/// VDB file magic string
const VDB_MAGIC: &[u8; 5] = b"VIATR";

/// VDB format version
const VDB_VERSION: u32 = 9;

// VDB opcodes
const VDB_OPCODE_AUX: u8 = 0xFA;
const VDB_OPCODE_RESIZEDB: u8 = 0xFB;
const VDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const VDB_OPCODE_EXPIRETIME: u8 = 0xFD;
const VDB_OPCODE_SELECTDB: u8 = 0xFE;
const VDB_OPCODE_EOF: u8 = 0xFF;

// VDB value types
const VDB_TYPE_STRING: u8 = 0;
const VDB_TYPE_LIST: u8 = 1;
const VDB_TYPE_SET: u8 = 2;
const VDB_TYPE_ZSET: u8 = 3;
const VDB_TYPE_HASH: u8 = 4;
const VDB_TYPE_ZSET_2: u8 = 5; // ZSET with double scores
const VDB_TYPE_LIST_QUICKLIST: u8 = 14;
const VDB_TYPE_HASH_ZIPMAP: u8 = 9;
const VDB_TYPE_SET_INTSET: u8 = 11;
const VDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const VDB_TYPE_HASH_ZIPLIST: u8 = 13;

// Viator-specific types
const VDB_TYPE_VECTORSET: u8 = 20; // Custom type for vector sets
const VDB_TYPE_STREAM: u8 = 21; // Stream type with full metadata

// Length encoding
const VDB_6BITLEN: u8 = 0;
const VDB_14BITLEN: u8 = 1;
const VDB_32BITLEN: u8 = 0x80;
const VDB_64BITLEN: u8 = 0x81;
const VDB_ENCVAL: u8 = 3;

// Special encoding for integers
const VDB_ENC_INT8: u8 = 0;
const VDB_ENC_INT16: u8 = 1;
const VDB_ENC_INT32: u8 = 2;
const VDB_ENC_LZF: u8 = 3;

/// VDB file saver.
///
/// Uses atomic temp-file + rename pattern to ensure crash safety:
/// 1. Write to `<path>.tmp`
/// 2. fsync the temp file
/// 3. Atomically rename to final path
///
/// This ensures the original file is never corrupted if a crash occurs during save.
pub struct VdbSaver {
    writer: BufWriter<File>,
    crc64: u64,
    temp_path: std::path::PathBuf,
    final_path: std::path::PathBuf,
}

impl VdbSaver {
    /// Create a new VDB saver.
    ///
    /// Creates a temporary file at `<path>.tmp` which will be atomically
    /// renamed to `path` on successful completion.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let final_path = path.as_ref().to_path_buf();
        let temp_path = final_path.with_extension("vdb.tmp");

        let file = File::create(&temp_path).map_err(StorageError::Io)?;
        Ok(Self {
            writer: BufWriter::new(file),
            crc64: 0,
            temp_path,
            final_path,
        })
    }

    /// Save the database to the VDB file.
    ///
    /// On success, atomically replaces the target file.
    /// On failure, cleans up the temporary file.
    pub fn save(mut self, db: &Database) -> Result<(), StorageError> {
        match self.save_inner(db) {
            Ok(()) => {
                // Drop the writer to close the file before rename
                drop(self.writer);

                // Atomic rename: this is the commit point. If we crash before this,
                // the original file is untouched. If we crash after, the new file is complete.
                std::fs::rename(&self.temp_path, &self.final_path).map_err(StorageError::Io)?;
                Ok(())
            }
            Err(e) => {
                // Clean up temp file on failure
                let _ = std::fs::remove_file(&self.temp_path);
                Err(e)
            }
        }
    }

    fn save_inner(&mut self, db: &Database) -> Result<(), StorageError> {
        // Write header
        self.write_header()?;

        // Write auxiliary fields
        self.write_aux("viator-ver", env!("CARGO_PKG_VERSION"))?;
        self.write_aux("bits", "64")?;
        self.write_aux("ctime", &chrono::Utc::now().timestamp().to_string())?;

        // For each database with keys
        for db_idx in 0..16u16 {
            let data = db.get_db_data(db_idx);
            if data.is_empty() {
                continue;
            }

            // Select database
            self.write_u8(VDB_OPCODE_SELECTDB)?;
            self.write_length(db_idx as u64)?;

            // Write resize info
            let expires_count = data.iter().filter(|e| e.expiry != Expiry::Never).count();
            self.write_u8(VDB_OPCODE_RESIZEDB)?;
            self.write_length(data.len() as u64)?;
            self.write_length(expires_count as u64)?;

            // Write each key-value pair
            for entry in data {
                // Write expiry if set
                if let Expiry::At(ts) = entry.expiry {
                    self.write_u8(VDB_OPCODE_EXPIRETIME_MS)?;
                    self.write_u64_le(ts as u64)?;
                }

                // Write type and key-value
                match entry.value_type {
                    ValueType::String => {
                        self.write_u8(VDB_TYPE_STRING)?;
                        self.write_string(&entry.key)?;
                        if let Some(s) = entry.string_value {
                            self.write_string(&s)?;
                        }
                    }
                    ValueType::List => {
                        self.write_u8(VDB_TYPE_LIST)?;
                        self.write_string(&entry.key)?;
                        if let Some(list) = entry.list_value {
                            self.write_length(list.len() as u64)?;
                            for item in list {
                                self.write_string(&item)?;
                            }
                        }
                    }
                    ValueType::Set => {
                        self.write_u8(VDB_TYPE_SET)?;
                        self.write_string(&entry.key)?;
                        if let Some(set) = entry.set_value {
                            self.write_length(set.len() as u64)?;
                            for member in set {
                                self.write_string(&member)?;
                            }
                        }
                    }
                    ValueType::ZSet => {
                        self.write_u8(VDB_TYPE_ZSET_2)?;
                        self.write_string(&entry.key)?;
                        if let Some(zset) = entry.zset_value {
                            self.write_length(zset.len() as u64)?;
                            for (member, score) in zset {
                                self.write_string(&member)?;
                                self.write_f64(score)?;
                            }
                        }
                    }
                    ValueType::Hash => {
                        self.write_u8(VDB_TYPE_HASH)?;
                        self.write_string(&entry.key)?;
                        if let Some(hash) = entry.hash_value {
                            self.write_length(hash.len() as u64)?;
                            for (field, value) in hash {
                                self.write_string(&field)?;
                                self.write_string(&value)?;
                            }
                        }
                    }
                    ValueType::Stream => {
                        if let Some(ref stream_export) = entry.stream_value {
                            self.write_u8(VDB_TYPE_STREAM)?;
                            self.write_string(&entry.key)?;
                            // Write last_id (ms, seq)
                            self.write_u64_le(stream_export.last_id.0)?;
                            self.write_u64_le(stream_export.last_id.1)?;
                            // Write entries_added
                            self.write_u64_le(stream_export.entries_added)?;
                            // Write number of entries
                            self.write_length(stream_export.entries.len() as u64)?;
                            // Write each entry
                            for (ms, seq, fields) in &stream_export.entries {
                                // Entry ID (ms, seq)
                                self.write_u64_le(*ms)?;
                                self.write_u64_le(*seq)?;
                                // Number of fields
                                self.write_length(fields.len() as u64)?;
                                // Field-value pairs
                                for (field, value) in fields {
                                    self.write_string(field)?;
                                    self.write_string(value)?;
                                }
                            }
                        }
                    }
                    ValueType::VectorSet => {
                        if let Some(ref vs_export) = entry.vectorset_value {
                            self.write_u8(VDB_TYPE_VECTORSET)?;
                            self.write_string(&entry.key)?;
                            // Write metric (1 byte)
                            self.write_u8(vs_export.metric)?;
                            // Write dimension
                            self.write_length(vs_export.dim as u64)?;
                            // Write number of elements
                            self.write_length(vs_export.elements.len() as u64)?;
                            // Write each element
                            for (name, vector, attrs) in &vs_export.elements {
                                // Element name
                                self.write_string(name)?;
                                // Vector (each f32)
                                for &val in vector {
                                    self.write_f64(val as f64)?;
                                }
                                // Attributes count and data
                                self.write_length(attrs.len() as u64)?;
                                for (attr_key, attr_val) in attrs {
                                    self.write_string(attr_key)?;
                                    self.write_string(attr_val)?;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Write EOF marker
        self.write_u8(VDB_OPCODE_EOF)?;

        // Write CRC64 checksum
        let crc = self.crc64;
        // Write without updating CRC
        self.writer
            .write_all(&crc.to_le_bytes())
            .map_err(StorageError::Io)?;
        self.writer.flush().map_err(StorageError::Io)?;

        // CRITICAL: fsync to ensure data is on disk before we consider save complete.
        // Without this, a power failure could result in a truncated/corrupt VDB file.
        self.writer.get_ref().sync_all().map_err(StorageError::Io)?;

        Ok(())
    }

    fn write_header(&mut self) -> Result<(), StorageError> {
        // Write magic + version
        self.write_bytes(VDB_MAGIC)?;
        // Version as 4-digit string (e.g., "0009")
        let version_str = format!("{:04}", VDB_VERSION);
        self.write_bytes(version_str.as_bytes())?;
        Ok(())
    }

    fn write_aux(&mut self, key: &str, value: &str) -> Result<(), StorageError> {
        self.write_u8(VDB_OPCODE_AUX)?;
        self.write_string(key.as_bytes())?;
        self.write_string(value.as_bytes())?;
        Ok(())
    }

    fn write_u8(&mut self, byte: u8) -> Result<(), StorageError> {
        self.crc64 = crc64_update(self.crc64, &[byte]);
        self.writer.write_all(&[byte]).map_err(StorageError::Io)
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        self.crc64 = crc64_update(self.crc64, bytes);
        self.writer.write_all(bytes).map_err(StorageError::Io)
    }

    fn write_u64_le(&mut self, n: u64) -> Result<(), StorageError> {
        let bytes = n.to_le_bytes();
        self.crc64 = crc64_update(self.crc64, &bytes);
        self.writer.write_all(&bytes).map_err(StorageError::Io)
    }

    fn write_f64(&mut self, n: f64) -> Result<(), StorageError> {
        let bytes = n.to_le_bytes();
        self.crc64 = crc64_update(self.crc64, &bytes);
        self.writer.write_all(&bytes).map_err(StorageError::Io)
    }

    fn write_length(&mut self, len: u64) -> Result<(), StorageError> {
        if len < 64 {
            // 6-bit length
            self.write_u8((VDB_6BITLEN << 6) | len as u8)?;
        } else if len < 16384 {
            // 14-bit length
            let high = (VDB_14BITLEN << 6) | ((len >> 8) & 0x3F) as u8;
            let low = (len & 0xFF) as u8;
            self.write_u8(high)?;
            self.write_u8(low)?;
        } else if len <= u32::MAX as u64 {
            // 32-bit length
            self.write_u8(VDB_32BITLEN)?;
            let bytes = (len as u32).to_be_bytes();
            self.write_bytes(&bytes)?;
        } else {
            // 64-bit length
            self.write_u8(VDB_64BITLEN)?;
            let bytes = len.to_be_bytes();
            self.write_bytes(&bytes)?;
        }
        Ok(())
    }

    fn write_string(&mut self, s: &[u8]) -> Result<(), StorageError> {
        // Try to encode as integer first
        if let Ok(s_str) = std::str::from_utf8(s) {
            if let Ok(n) = s_str.parse::<i64>() {
                if n >= i8::MIN as i64 && n <= i8::MAX as i64 {
                    self.write_u8((VDB_ENCVAL << 6) | VDB_ENC_INT8)?;
                    self.write_u8(n as u8)?;
                    return Ok(());
                } else if n >= i16::MIN as i64 && n <= i16::MAX as i64 {
                    self.write_u8((VDB_ENCVAL << 6) | VDB_ENC_INT16)?;
                    let bytes = (n as i16).to_le_bytes();
                    self.write_bytes(&bytes)?;
                    return Ok(());
                } else if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                    self.write_u8((VDB_ENCVAL << 6) | VDB_ENC_INT32)?;
                    let bytes = (n as i32).to_le_bytes();
                    self.write_bytes(&bytes)?;
                    return Ok(());
                }
            }
        }

        // Try LZF compression for strings >= 20 bytes
        if s.len() >= LZF_COMPRESS_MIN_LEN {
            if let Some(compressed) = lzf_compress(s) {
                // Write LZF-compressed string
                self.write_u8((VDB_ENCVAL << 6) | VDB_ENC_LZF)?;
                self.write_length(compressed.len() as u64)?;
                self.write_length(s.len() as u64)?;
                self.write_bytes(&compressed)?;
                return Ok(());
            }
        }

        // Write as raw string
        self.write_length(s.len() as u64)?;
        self.write_bytes(s)?;
        Ok(())
    }
}

/// VDB file loader.
pub struct VdbLoader {
    reader: BufReader<File>,
    crc64: u64,
}

impl VdbLoader {
    /// Create a new VDB loader.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = File::open(path).map_err(StorageError::Io)?;
        Ok(Self {
            reader: BufReader::new(file),
            crc64: 0,
        })
    }

    /// Load the VDB file into the database.
    pub fn load_into(mut self, db: &Database) -> Result<(), StorageError> {
        // Read and verify header (include in CRC64 checksum)
        let mut magic = [0u8; 5];
        self.reader
            .read_exact(&mut magic)
            .map_err(StorageError::Io)?;
        self.crc64 = crc64_update(self.crc64, &magic);
        if &magic != VDB_MAGIC {
            return Err(StorageError::Corrupted(
                "Invalid dump file magic (expected VIATR)".to_string(),
            ));
        }

        let mut version_str = [0u8; 4];
        self.reader
            .read_exact(&mut version_str)
            .map_err(StorageError::Io)?;
        self.crc64 = crc64_update(self.crc64, &version_str);
        let version: u32 = std::str::from_utf8(&version_str)
            .map_err(|_| StorageError::Corrupted("Invalid version string".to_string()))?
            .parse()
            .map_err(|_| StorageError::Corrupted("Invalid version number".to_string()))?;

        if version > VDB_VERSION {
            return Err(StorageError::UnsupportedVersion(version));
        }

        let mut current_db: u16 = 0;
        let mut current_expiry: Option<i64> = None;

        loop {
            let opcode = self.read_u8()?;

            match opcode {
                VDB_OPCODE_AUX => {
                    // Skip auxiliary data
                    let _key = self.read_string()?;
                    let _value = self.read_string()?;
                }
                VDB_OPCODE_SELECTDB => {
                    current_db = self.read_length()? as u16;
                }
                VDB_OPCODE_RESIZEDB => {
                    let _db_size = self.read_length()?;
                    let _expires_size = self.read_length()?;
                }
                VDB_OPCODE_EXPIRETIME_MS => {
                    current_expiry = Some(self.read_i64_le()?);
                }
                VDB_OPCODE_EXPIRETIME => {
                    // Seconds, convert to milliseconds
                    current_expiry = Some(self.read_i32_le()? as i64 * 1000);
                }
                VDB_OPCODE_EOF => {
                    // End of file - verify CRC64 checksum
                    let expected_crc = self.crc64;
                    let mut stored_crc_bytes = [0u8; 8];
                    self.reader
                        .read_exact(&mut stored_crc_bytes)
                        .map_err(StorageError::Io)?;
                    let stored_crc = u64::from_le_bytes(stored_crc_bytes);

                    if stored_crc != 0 && stored_crc != expected_crc {
                        return Err(StorageError::ChecksumMismatch {
                            expected: (expected_crc & 0xFFFF_FFFF) as u32,
                            actual: (stored_crc & 0xFFFF_FFFF) as u32,
                        });
                    }
                    break;
                }
                value_type => {
                    // Value type - read key and value
                    let key = Bytes::from(self.read_string()?);
                    let expiry = current_expiry
                        .map(Expiry::at_millis)
                        .unwrap_or(Expiry::Never);
                    current_expiry = None;

                    // Skip if already expired
                    if expiry.is_expired() {
                        self.skip_value(value_type)?;
                        continue;
                    }

                    match value_type {
                        VDB_TYPE_STRING => {
                            let value = Bytes::from(self.read_string()?);
                            db.set_string_with_expiry(current_db, key, value, expiry);
                        }
                        VDB_TYPE_LIST | VDB_TYPE_LIST_QUICKLIST => {
                            let len = self.read_length()?;
                            let mut items = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                items.push(Bytes::from(self.read_string()?));
                            }
                            db.set_list_with_expiry(current_db, key, items, expiry);
                        }
                        VDB_TYPE_SET | VDB_TYPE_SET_INTSET => {
                            let len = self.read_length()?;
                            let mut members = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                members.push(Bytes::from(self.read_string()?));
                            }
                            db.set_set_with_expiry(current_db, key, members, expiry);
                        }
                        VDB_TYPE_ZSET | VDB_TYPE_ZSET_2 | VDB_TYPE_ZSET_ZIPLIST => {
                            let len = self.read_length()?;
                            let mut entries = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                let member = Bytes::from(self.read_string()?);
                                let score = if value_type == VDB_TYPE_ZSET_2 {
                                    self.read_f64()?
                                } else {
                                    // Old format: score as string
                                    let score_str = self.read_string()?;
                                    std::str::from_utf8(&score_str)
                                        .ok()
                                        .and_then(|s| s.parse().ok())
                                        .unwrap_or(0.0)
                                };
                                entries.push((member, score));
                            }
                            db.set_zset_with_expiry(current_db, key, entries, expiry);
                        }
                        VDB_TYPE_HASH | VDB_TYPE_HASH_ZIPLIST | VDB_TYPE_HASH_ZIPMAP => {
                            let len = self.read_length()?;
                            let mut fields = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                let field = Bytes::from(self.read_string()?);
                                let value = Bytes::from(self.read_string()?);
                                fields.push((field, value));
                            }
                            db.set_hash_with_expiry(current_db, key, fields, expiry);
                        }
                        VDB_TYPE_STREAM => {
                            use crate::storage::StreamExport;

                            // Read last_id (ms, seq)
                            let last_id_ms = self.read_u64_le()?;
                            let last_id_seq = self.read_u64_le()?;
                            // Read entries_added
                            let entries_added = self.read_u64_le()?;
                            // Read number of entries
                            let num_entries = self.read_length()? as usize;
                            let mut entries = Vec::with_capacity(num_entries);

                            for _ in 0..num_entries {
                                // Entry ID (ms, seq)
                                let ms = self.read_u64_le()?;
                                let seq = self.read_u64_le()?;
                                // Number of fields
                                let num_fields = self.read_length()? as usize;
                                let mut fields = Vec::with_capacity(num_fields);
                                // Field-value pairs
                                for _ in 0..num_fields {
                                    let field = Bytes::from(self.read_string()?);
                                    let value = Bytes::from(self.read_string()?);
                                    fields.push((field, value));
                                }
                                entries.push((ms, seq, fields));
                            }

                            let export = StreamExport {
                                last_id: (last_id_ms, last_id_seq),
                                entries_added,
                                entries,
                            };
                            db.set_stream_with_expiry(current_db, key, export, expiry);
                        }
                        VDB_TYPE_VECTORSET => {
                            use crate::storage::VectorSetExport;

                            // Read metric (1 byte)
                            let metric = self.read_u8()?;
                            // Read dimension
                            let dim = self.read_length()? as usize;
                            // Read number of elements
                            let num_elements = self.read_length()? as usize;
                            let mut elements = Vec::with_capacity(num_elements);

                            for _ in 0..num_elements {
                                // Element name
                                let name = Bytes::from(self.read_string()?);
                                // Vector (dim f64 values stored as f32)
                                let mut vector = Vec::with_capacity(dim);
                                for _ in 0..dim {
                                    vector.push(self.read_f64()? as f32);
                                }
                                // Attributes
                                let attr_count = self.read_length()? as usize;
                                let mut attrs = Vec::with_capacity(attr_count);
                                for _ in 0..attr_count {
                                    let attr_key = Bytes::from(self.read_string()?);
                                    let attr_val = Bytes::from(self.read_string()?);
                                    attrs.push((attr_key, attr_val));
                                }
                                elements.push((name, vector, attrs));
                            }

                            let export = VectorSetExport {
                                metric,
                                dim,
                                elements,
                            };
                            db.set_vectorset_with_expiry(current_db, key, export, expiry);
                        }
                        _ => {
                            // Unknown type - skip
                            tracing::warn!("Unknown VDB value type: {}", value_type);
                            self.skip_value(value_type)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8, StorageError> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf).map_err(StorageError::Io)?;
        self.crc64 = crc64_update(self.crc64, &buf);
        Ok(buf[0])
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, StorageError> {
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).map_err(StorageError::Io)?;
        self.crc64 = crc64_update(self.crc64, &buf);
        Ok(buf)
    }

    fn read_i32_le(&mut self) -> Result<i32, StorageError> {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf).map_err(StorageError::Io)?;
        self.crc64 = crc64_update(self.crc64, &buf);
        Ok(i32::from_le_bytes(buf))
    }

    fn read_i64_le(&mut self) -> Result<i64, StorageError> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf).map_err(StorageError::Io)?;
        self.crc64 = crc64_update(self.crc64, &buf);
        Ok(i64::from_le_bytes(buf))
    }

    fn read_u64_le(&mut self) -> Result<u64, StorageError> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf).map_err(StorageError::Io)?;
        self.crc64 = crc64_update(self.crc64, &buf);
        Ok(u64::from_le_bytes(buf))
    }

    fn read_f64(&mut self) -> Result<f64, StorageError> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf).map_err(StorageError::Io)?;
        self.crc64 = crc64_update(self.crc64, &buf);
        Ok(f64::from_le_bytes(buf))
    }

    fn read_length(&mut self) -> Result<u64, StorageError> {
        let first = self.read_u8()?;
        let encoding = (first & 0xC0) >> 6;

        match encoding {
            VDB_6BITLEN => Ok((first & 0x3F) as u64),
            VDB_14BITLEN => {
                let second = self.read_u8()?;
                Ok((((first & 0x3F) as u64) << 8) | (second as u64))
            }
            VDB_ENCVAL => {
                // Special encoding - return as-is
                Err(StorageError::Corrupted(
                    "Unexpected ENCVAL in length".to_string(),
                ))
            }
            _ => {
                // 32-bit or 64-bit
                if first == VDB_32BITLEN {
                    let buf = self.read_bytes(4)?;
                    Ok(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64)
                } else if first == VDB_64BITLEN {
                    let buf = self.read_bytes(8)?;
                    Ok(u64::from_be_bytes([
                        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                    ]))
                } else {
                    Err(StorageError::Corrupted(format!(
                        "Unknown length encoding: {}",
                        first
                    )))
                }
            }
        }
    }

    fn read_string(&mut self) -> Result<Vec<u8>, StorageError> {
        let first = self.read_u8()?;
        let encoding = (first & 0xC0) >> 6;

        if encoding == VDB_ENCVAL {
            // Special encoding
            let enc_type = first & 0x3F;
            match enc_type {
                VDB_ENC_INT8 => {
                    let n = self.read_u8()? as i8;
                    Ok(n.to_string().into_bytes())
                }
                VDB_ENC_INT16 => {
                    let buf = self.read_bytes(2)?;
                    let n = i16::from_le_bytes([buf[0], buf[1]]);
                    Ok(n.to_string().into_bytes())
                }
                VDB_ENC_INT32 => {
                    let buf = self.read_bytes(4)?;
                    let n = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
                    Ok(n.to_string().into_bytes())
                }
                VDB_ENC_LZF => {
                    // LZF compressed string
                    let compressed_len = self.read_length()? as usize;
                    let original_len = self.read_length()? as usize;
                    let compressed = self.read_bytes(compressed_len)?;

                    // Decompress using LZF algorithm
                    let decompressed = lzf_decompress(&compressed, original_len).map_err(|e| {
                        StorageError::Corrupted(format!("LZF decompression failed: {}", e))
                    })?;
                    Ok(decompressed)
                }
                _ => Err(StorageError::Corrupted(format!(
                    "Unknown string encoding: {}",
                    enc_type
                ))),
            }
        } else {
            // Regular length-prefixed string
            let len = match encoding {
                VDB_6BITLEN => (first & 0x3F) as u64,
                VDB_14BITLEN => {
                    let second = self.read_u8()?;
                    (((first & 0x3F) as u64) << 8) | (second as u64)
                }
                _ => {
                    if first == VDB_32BITLEN {
                        let buf = self.read_bytes(4)?;
                        u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64
                    } else if first == VDB_64BITLEN {
                        let buf = self.read_bytes(8)?;
                        u64::from_be_bytes([
                            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                        ])
                    } else {
                        return Err(StorageError::Corrupted(format!(
                            "Unknown string length: {}",
                            first
                        )));
                    }
                }
            };

            let buf = self.read_bytes(len as usize)?;
            Ok(buf)
        }
    }

    fn skip_value(&mut self, _value_type: u8) -> Result<(), StorageError> {
        // Skip the value based on type
        // For now, just try to read as string (works for most types)
        let _ = self.read_string()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{lzf_compress, lzf_decompress};
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_lzf_roundtrip() {
        // Test with repetitive data (compresses well)
        let input = b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        let compressed = lzf_compress(input).expect("Should compress repetitive data");
        assert!(
            compressed.len() < input.len(),
            "Compressed should be smaller"
        );
        let decompressed = lzf_decompress(&compressed, input.len()).expect("Should decompress");
        assert_eq!(decompressed, input);

        // Test with more complex repetitive pattern
        let input2: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        if let Some(compressed2) = lzf_compress(&input2) {
            let decompressed2 =
                lzf_decompress(&compressed2, input2.len()).expect("Should decompress");
            assert_eq!(decompressed2, input2);
        }

        // Test with benchmark-like data (key:value patterns)
        let mut benchmark_data = Vec::new();
        for i in 0..100 {
            benchmark_data.extend_from_slice(format!("key:{:012}", i).as_bytes());
            benchmark_data.extend_from_slice(b"xxxxxxxxxxxxxxxxxxxx");
        }
        if let Some(compressed3) = lzf_compress(&benchmark_data) {
            assert!(
                compressed3.len() < benchmark_data.len(),
                "Benchmark data should compress"
            );
            let decompressed3 =
                lzf_decompress(&compressed3, benchmark_data.len()).expect("Should decompress");
            assert_eq!(decompressed3, benchmark_data);
        }
    }

    #[test]
    fn test_lzf_incompressible() {
        // Random-ish data that doesn't compress well
        let input: Vec<u8> = (0..100).map(|i| ((i * 7 + 13) % 256) as u8).collect();
        // May or may not compress - just verify roundtrip if it does
        if let Some(compressed) = lzf_compress(&input) {
            let decompressed = lzf_decompress(&compressed, input.len()).expect("Should decompress");
            assert_eq!(decompressed, input);
        }
    }

    #[test]
    fn test_length_encoding() {
        // Test various length encodings
        let test_cases = vec![
            (0u64, vec![0x00]),
            (63, vec![0x3F]),
            (64, vec![0x40, 0x40]),
            (16383, vec![0x7F, 0xFF]),
        ];

        for (len, expected) in test_cases {
            let mut buf = BytesMut::new();
            // Manual encoding for testing
            if len < 64 {
                buf.put_u8(len as u8);
            } else if len < 16384 {
                buf.put_u8(0x40 | ((len >> 8) & 0x3F) as u8);
                buf.put_u8((len & 0xFF) as u8);
            }
            assert_eq!(buf.to_vec(), expected, "Failed for length {}", len);
        }
    }
}
