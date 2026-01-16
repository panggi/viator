//! AOF (Append Only File) persistence implementation.
//!
//! AOF provides durability by logging every write command to a file.
//! Commands are stored in RESP format for easy replay.
//!
//! # Fsync Policies
//!
//! - **Always**: Fsync after every write (safest, slowest)
//! - **EverySec**: Fsync once per second (good balance)
//! - **No**: Let the OS handle fsyncing (fastest, least safe)

use crate::error::StorageError;
use crate::storage::Database;
use bytes::{Bytes, BytesMut};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// AOF fsync policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AofFsync {
    /// Fsync after every write command
    Always,
    /// Fsync once per second
    #[default]
    EverySec,
    /// Never explicitly fsync (let the OS decide)
    No,
}

impl AofFsync {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "always" => Some(Self::Always),
            "everysec" => Some(Self::EverySec),
            "no" => Some(Self::No),
            _ => None,
        }
    }
}

/// AOF writer for appending commands to the AOF file.
pub struct AofWriter {
    writer: BufWriter<File>,
    fsync_policy: AofFsync,
    last_fsync: Instant,
    bytes_since_fsync: u64,
    total_bytes: AtomicU64,
    needs_fsync: AtomicBool,
}

impl AofWriter {
    /// Create a new AOF writer.
    pub fn new<P: AsRef<Path>>(path: P, fsync_policy: AofFsync) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(StorageError::Io)?;

        Ok(Self {
            writer: BufWriter::with_capacity(64 * 1024, file),
            fsync_policy,
            last_fsync: Instant::now(),
            bytes_since_fsync: 0,
            total_bytes: AtomicU64::new(0),
            needs_fsync: AtomicBool::new(false),
        })
    }

    /// Append a command (as raw RESP bytes) to the AOF.
    pub fn append(&mut self, command: &[u8]) -> Result<(), StorageError> {
        self.writer.write_all(command).map_err(StorageError::Io)?;
        self.bytes_since_fsync += command.len() as u64;
        self.total_bytes
            .fetch_add(command.len() as u64, Ordering::Relaxed);
        self.needs_fsync.store(true, Ordering::Relaxed);

        // Handle fsync based on policy
        match self.fsync_policy {
            AofFsync::Always => {
                self.fsync()?;
            }
            AofFsync::EverySec => {
                if self.last_fsync.elapsed() >= Duration::from_secs(1) {
                    self.fsync()?;
                }
            }
            AofFsync::No => {
                // Just flush the buffer, no fsync
                self.writer.flush().map_err(StorageError::Io)?;
            }
        }

        Ok(())
    }

    /// Append a parsed command to the AOF.
    pub fn append_command(&mut self, command: &[Bytes]) -> Result<(), StorageError> {
        // Serialize to RESP array
        let mut buf = BytesMut::with_capacity(256);

        // Array header
        buf.extend_from_slice(format!("*{}\r\n", command.len()).as_bytes());

        // Each argument as bulk string
        for arg in command {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }

        self.append(&buf)
    }

    /// Force fsync.
    pub fn fsync(&mut self) -> Result<(), StorageError> {
        if self.needs_fsync.load(Ordering::Relaxed) {
            self.writer.flush().map_err(StorageError::Io)?;
            self.writer
                .get_ref()
                .sync_data()
                .map_err(StorageError::Io)?;
            self.last_fsync = Instant::now();
            self.bytes_since_fsync = 0;
            self.needs_fsync.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Get total bytes written.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Rewrite the AOF from the current database state.
    ///
    /// This creates a new AOF containing only the commands needed to
    /// recreate the current state, removing redundant operations.
    ///
    /// # Safety
    ///
    /// This function uses atomic temp-file + rename pattern to ensure
    /// crash-safety. If a crash occurs during rewrite, the original
    /// AOF file remains intact.
    pub fn rewrite_from_db<P: AsRef<Path>>(path: P, db: &Database) -> Result<(), StorageError> {
        let path = path.as_ref();

        // Create a unique temp file in the same directory (for atomic rename)
        let temp_path = path.with_extension(format!("aof.temp.{}", std::process::id()));

        // Ensure temp file is cleaned up on error
        let result = Self::write_aof_to_file(&temp_path, db);

        if let Err(e) = result {
            // Clean up temp file on error
            let _ = std::fs::remove_file(&temp_path);
            return Err(e);
        }

        // Atomic rename: replace old AOF with new one
        std::fs::rename(&temp_path, path).map_err(StorageError::Io)?;

        Ok(())
    }

    /// Internal helper to write AOF content to a file.
    fn write_aof_to_file<P: AsRef<Path>>(path: P, db: &Database) -> Result<(), StorageError> {
        let file = File::create(path).map_err(StorageError::Io)?;
        let mut writer = BufWriter::with_capacity(64 * 1024, file);

        // For each database
        for db_idx in 0..16u16 {
            let data = db.get_db_data(db_idx);
            if data.is_empty() {
                continue;
            }

            // SELECT command
            Self::write_command(&mut writer, &["SELECT", &db_idx.to_string()])?;

            // Write each key
            for entry in data {
                // Skip expired keys
                if entry.expiry.is_expired() {
                    continue;
                }

                let key_str = String::from_utf8_lossy(&entry.key);

                match entry.value_type {
                    crate::types::ValueType::String => {
                        if let Some(value) = entry.string_value {
                            Self::write_command(
                                &mut writer,
                                &["SET", &key_str, &String::from_utf8_lossy(&value)],
                            )?;
                        }
                    }
                    crate::types::ValueType::List => {
                        if let Some(list) = entry.list_value {
                            if !list.is_empty() {
                                // Use RPUSH to restore list
                                let mut cmd = vec!["RPUSH".to_string(), key_str.to_string()];
                                for item in list {
                                    cmd.push(String::from_utf8_lossy(&item).to_string());
                                }
                                let cmd_refs: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
                                Self::write_command(&mut writer, &cmd_refs)?;
                            }
                        }
                    }
                    crate::types::ValueType::Set => {
                        if let Some(set) = entry.set_value {
                            if !set.is_empty() {
                                // Use SADD to restore set
                                let mut cmd = vec!["SADD".to_string(), key_str.to_string()];
                                for member in set {
                                    cmd.push(String::from_utf8_lossy(&member).to_string());
                                }
                                let cmd_refs: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
                                Self::write_command(&mut writer, &cmd_refs)?;
                            }
                        }
                    }
                    crate::types::ValueType::ZSet => {
                        if let Some(zset) = entry.zset_value {
                            if !zset.is_empty() {
                                // Use ZADD to restore sorted set
                                let mut cmd = vec!["ZADD".to_string(), key_str.to_string()];
                                for (member, score) in zset {
                                    cmd.push(score.to_string());
                                    cmd.push(String::from_utf8_lossy(&member).to_string());
                                }
                                let cmd_refs: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
                                Self::write_command(&mut writer, &cmd_refs)?;
                            }
                        }
                    }
                    crate::types::ValueType::Hash => {
                        if let Some(hash) = entry.hash_value {
                            if !hash.is_empty() {
                                // Use HSET to restore hash
                                let mut cmd = vec!["HSET".to_string(), key_str.to_string()];
                                for (field, value) in hash {
                                    cmd.push(String::from_utf8_lossy(&field).to_string());
                                    cmd.push(String::from_utf8_lossy(&value).to_string());
                                }
                                let cmd_refs: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
                                Self::write_command(&mut writer, &cmd_refs)?;
                            }
                        }
                    }
                    crate::types::ValueType::Stream => {
                        // Skip streams for now
                        continue;
                    }
                    crate::types::ValueType::VectorSet => {
                        if let Some(ref vs_export) = entry.vectorset_value {
                            // For each element, write VADD command
                            // Format: VADD key FP32 VALUES dim v1 v2 ... vn element [SETATTR attr1 val1 ...]
                            for (name, vector, attrs) in &vs_export.elements {
                                let mut cmd = vec![
                                    "VADD".to_string(),
                                    key_str.to_string(),
                                    "FP32".to_string(),
                                    "VALUES".to_string(),
                                    vs_export.dim.to_string(),
                                ];
                                // Add vector values
                                for val in vector {
                                    cmd.push(val.to_string());
                                }
                                // Add element name
                                cmd.push(String::from_utf8_lossy(name).to_string());
                                // Add attributes if any
                                if !attrs.is_empty() {
                                    cmd.push("SETATTR".to_string());
                                    for (attr_key, attr_val) in attrs {
                                        cmd.push(String::from_utf8_lossy(attr_key).to_string());
                                        cmd.push(String::from_utf8_lossy(attr_val).to_string());
                                    }
                                }
                                let cmd_refs: Vec<&str> = cmd.iter().map(|s| s.as_str()).collect();
                                Self::write_command(&mut writer, &cmd_refs)?;
                            }
                        }
                    }
                }

                // Set expiry if needed
                if let crate::types::Expiry::At(ts) = entry.expiry {
                    Self::write_command(&mut writer, &["PEXPIREAT", &key_str, &ts.to_string()])?;
                }
            }
        }

        writer.flush().map_err(StorageError::Io)?;
        writer.get_ref().sync_all().map_err(StorageError::Io)?;

        Ok(())
    }

    /// Write a command in RESP format.
    fn write_command<W: Write>(writer: &mut W, args: &[&str]) -> Result<(), StorageError> {
        // Array header
        write!(writer, "*{}\r\n", args.len()).map_err(|e| StorageError::Io(e))?;

        // Each argument as bulk string
        for arg in args {
            write!(writer, "${}\r\n{}\r\n", arg.len(), arg).map_err(|e| StorageError::Io(e))?;
        }

        Ok(())
    }
}

/// AOF file reader for replay.
pub struct AofReader {
    data: Vec<u8>,
    pos: usize,
}

impl AofReader {
    /// Create a new AOF reader.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let data = std::fs::read(path).map_err(StorageError::Io)?;
        Ok(Self { data, pos: 0 })
    }

    /// Read the next command from the AOF.
    pub fn next_command(&mut self) -> Result<Option<Vec<Bytes>>, StorageError> {
        if self.pos >= self.data.len() {
            return Ok(None);
        }

        // Skip any whitespace/newlines
        while self.pos < self.data.len()
            && (self.data[self.pos] == b'\r' || self.data[self.pos] == b'\n')
        {
            self.pos += 1;
        }

        if self.pos >= self.data.len() {
            return Ok(None);
        }

        // Expect array marker
        if self.data[self.pos] != b'*' {
            return Err(StorageError::Corrupted(format!(
                "Expected '*', got '{}'",
                self.data[self.pos] as char
            )));
        }
        self.pos += 1;

        // Read array length
        let array_len = self.read_integer()? as usize;
        let mut args = Vec::with_capacity(array_len);

        for _ in 0..array_len {
            // Skip CRLF
            self.skip_crlf()?;

            // Expect bulk string marker
            if self.data[self.pos] != b'$' {
                return Err(StorageError::Corrupted(format!(
                    "Expected '$', got '{}'",
                    self.data[self.pos] as char
                )));
            }
            self.pos += 1;

            // Read string length
            let str_len = self.read_integer()? as usize;
            self.skip_crlf()?;

            // Read string data
            if self.pos + str_len > self.data.len() {
                return Err(StorageError::Corrupted("Unexpected end of AOF".to_string()));
            }
            args.push(Bytes::copy_from_slice(
                &self.data[self.pos..self.pos + str_len],
            ));
            self.pos += str_len;
        }

        // Skip trailing CRLF
        self.skip_crlf().ok();

        Ok(Some(args))
    }

    fn read_integer(&mut self) -> Result<i64, StorageError> {
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != b'\r' {
            self.pos += 1;
        }

        let s = std::str::from_utf8(&self.data[start..self.pos])
            .map_err(|_| StorageError::Corrupted("Invalid integer in AOF".to_string()))?;
        s.parse()
            .map_err(|_| StorageError::Corrupted(format!("Invalid integer: {}", s)))
    }

    fn skip_crlf(&mut self) -> Result<(), StorageError> {
        if self.pos < self.data.len() && self.data[self.pos] == b'\r' {
            self.pos += 1;
        }
        if self.pos < self.data.len() && self.data[self.pos] == b'\n' {
            self.pos += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aof_fsync_from_str() {
        assert_eq!(AofFsync::from_str("always"), Some(AofFsync::Always));
        assert_eq!(AofFsync::from_str("EVERYSEC"), Some(AofFsync::EverySec));
        assert_eq!(AofFsync::from_str("No"), Some(AofFsync::No));
        assert_eq!(AofFsync::from_str("invalid"), None);
    }

    #[test]
    fn test_write_command() {
        let mut buf = Vec::new();
        AofWriter::write_command(&mut buf, &["SET", "key", "value"]).unwrap();
        assert_eq!(buf, b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    }
}
