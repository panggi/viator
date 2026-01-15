//! Viator Check VDB - VDB file validation tool
//!
//! A tool for checking and validating Viator VDB dump files.
//! Verifies file integrity, checksums, and reports any corruption.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

/// CRC64 polynomial (ECMA-182)
const CRC64_POLY: u64 = 0xC96C5795D7870F42;

/// CRC64 lookup table for fast computation
fn crc64_table() -> [u64; 256] {
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
}

/// Calculate CRC64 checksum
fn crc64_update(table: &[u64; 256], crc: u64, data: &[u8]) -> u64 {
    let mut crc = crc;
    for byte in data {
        let idx = ((crc ^ (*byte as u64)) & 0xFF) as usize;
        crc = table[idx] ^ (crc >> 8);
    }
    crc
}

/// VDB opcodes
const VDB_OPCODE_AUX: u8 = 0xFA;
const VDB_OPCODE_RESIZEDB: u8 = 0xFB;
const VDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const VDB_OPCODE_EXPIRETIME: u8 = 0xFD;
const VDB_OPCODE_SELECTDB: u8 = 0xFE;
const VDB_OPCODE_EOF: u8 = 0xFF;

/// VDB value types
const VDB_TYPE_STRING: u8 = 0;
const VDB_TYPE_LIST: u8 = 1;
const VDB_TYPE_SET: u8 = 2;
const VDB_TYPE_ZSET: u8 = 3;
const VDB_TYPE_HASH: u8 = 4;
const VDB_TYPE_ZSET_2: u8 = 5;
const VDB_TYPE_LIST_QUICKLIST: u8 = 14;
const VDB_TYPE_HASH_ZIPMAP: u8 = 9;
const VDB_TYPE_SET_INTSET: u8 = 11;
const VDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const VDB_TYPE_HASH_ZIPLIST: u8 = 13;
const VDB_TYPE_VECTORSET: u8 = 20;

/// Length encoding markers
const VDB_6BITLEN: u8 = 0;
const VDB_14BITLEN: u8 = 1;
const VDB_32BITLEN: u8 = 0x80;
const VDB_64BITLEN: u8 = 0x81;
const VDB_ENCVAL: u8 = 3;

/// Special encoding for integers
const VDB_ENC_INT8: u8 = 0;
const VDB_ENC_INT16: u8 = 1;
const VDB_ENC_INT32: u8 = 2;
const VDB_ENC_LZF: u8 = 3;

/// VDB file checker
struct VdbChecker {
    reader: BufReader<File>,
    crc64: u64,
    crc64_table: [u64; 256],
    position: u64,
    verbose: bool,
    // Statistics
    total_keys: u64,
    string_keys: u64,
    list_keys: u64,
    set_keys: u64,
    zset_keys: u64,
    hash_keys: u64,
    other_keys: u64,
    expires: u64,
    databases: Vec<u16>,
}

impl VdbChecker {
    fn new(path: &PathBuf, verbose: bool) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("Cannot open file: {}", e))?;
        Ok(Self {
            reader: BufReader::new(file),
            crc64: 0,
            crc64_table: crc64_table(),
            position: 0,
            verbose,
            total_keys: 0,
            string_keys: 0,
            list_keys: 0,
            set_keys: 0,
            zset_keys: 0,
            hash_keys: 0,
            other_keys: 0,
            expires: 0,
            databases: Vec::new(),
        })
    }

    fn read_byte(&mut self) -> Result<u8, String> {
        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf).map_err(|e| format!("Read error at {}: {}", self.position, e))?;
        self.crc64 = crc64_update(&self.crc64_table, self.crc64, &buf);
        self.position += 1;
        Ok(buf[0])
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, String> {
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).map_err(|e| format!("Read error at {}: {}", self.position, e))?;
        self.crc64 = crc64_update(&self.crc64_table, self.crc64, &buf);
        self.position += len as u64;
        Ok(buf)
    }

    fn read_length(&mut self) -> Result<u64, String> {
        let first = self.read_byte()?;
        let encoding = (first & 0xC0) >> 6;

        match encoding {
            VDB_6BITLEN => Ok((first & 0x3F) as u64),
            VDB_14BITLEN => {
                let second = self.read_byte()?;
                Ok((((first & 0x3F) as u64) << 8) | (second as u64))
            }
            VDB_ENCVAL => {
                // Special encoding - return the type
                Err(format!("Unexpected ENCVAL in length at position {}", self.position))
            }
            _ => {
                if first == VDB_32BITLEN {
                    let buf = self.read_bytes(4)?;
                    Ok(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64)
                } else if first == VDB_64BITLEN {
                    let buf = self.read_bytes(8)?;
                    Ok(u64::from_be_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]]))
                } else {
                    Err(format!("Unknown length encoding {} at position {}", first, self.position))
                }
            }
        }
    }

    fn read_string(&mut self) -> Result<Vec<u8>, String> {
        let first = self.read_byte()?;
        let encoding = (first & 0xC0) >> 6;

        if encoding == VDB_ENCVAL {
            let enc_type = first & 0x3F;
            match enc_type {
                VDB_ENC_INT8 => {
                    let n = self.read_byte()? as i8;
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
                    let compressed_len = self.read_length()? as usize;
                    let _original_len = self.read_length()? as usize;
                    let _compressed = self.read_bytes(compressed_len)?;
                    // Just validate we can read it, don't decompress
                    Ok(vec![])
                }
                _ => Err(format!("Unknown string encoding {} at position {}", enc_type, self.position)),
            }
        } else {
            let len = match encoding {
                VDB_6BITLEN => (first & 0x3F) as u64,
                VDB_14BITLEN => {
                    let second = self.read_byte()?;
                    (((first & 0x3F) as u64) << 8) | (second as u64)
                }
                _ => {
                    if first == VDB_32BITLEN {
                        let buf = self.read_bytes(4)?;
                        u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64
                    } else if first == VDB_64BITLEN {
                        let buf = self.read_bytes(8)?;
                        u64::from_be_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]])
                    } else {
                        return Err(format!("Unknown string length {} at position {}", first, self.position));
                    }
                }
            };

            self.read_bytes(len as usize)
        }
    }

    fn skip_value(&mut self, value_type: u8) -> Result<(), String> {
        match value_type {
            VDB_TYPE_STRING => {
                self.read_string()?;
            }
            VDB_TYPE_LIST | VDB_TYPE_SET | VDB_TYPE_LIST_QUICKLIST | VDB_TYPE_SET_INTSET => {
                let len = self.read_length()?;
                for _ in 0..len {
                    self.read_string()?;
                }
            }
            VDB_TYPE_ZSET | VDB_TYPE_ZSET_2 | VDB_TYPE_ZSET_ZIPLIST => {
                let len = self.read_length()?;
                for _ in 0..len {
                    self.read_string()?;
                    if value_type == VDB_TYPE_ZSET_2 {
                        self.read_bytes(8)?; // f64
                    } else {
                        self.read_string()?; // score as string
                    }
                }
            }
            VDB_TYPE_HASH | VDB_TYPE_HASH_ZIPLIST | VDB_TYPE_HASH_ZIPMAP => {
                let len = self.read_length()?;
                for _ in 0..len {
                    self.read_string()?;
                    self.read_string()?;
                }
            }
            VDB_TYPE_VECTORSET => {
                let _metric = self.read_byte()?;
                let dim = self.read_length()? as usize;
                let num_elements = self.read_length()? as usize;
                for _ in 0..num_elements {
                    self.read_string()?; // element name
                    self.read_bytes(dim * 8)?; // vector (f64 each)
                    let attr_count = self.read_length()? as usize;
                    for _ in 0..attr_count {
                        self.read_string()?;
                        self.read_string()?;
                    }
                }
            }
            _ => {
                // Unknown type - try to read as string
                self.read_string()?;
            }
        }
        Ok(())
    }

    fn check(&mut self) -> Result<(), String> {
        // Check magic header
        let magic = self.read_bytes(5)?;
        let magic_str = String::from_utf8_lossy(&magic);

        if &magic != b"VIATR" && &magic != b"REDIS" {
            return Err(format!("Invalid magic header: expected 'VIATR' or 'REDIS', got '{}'", magic_str));
        }

        if self.verbose {
            println!("[offset 0] Magic: {}", magic_str);
        }

        // Check version
        let version_bytes = self.read_bytes(4)?;
        let version_str = String::from_utf8_lossy(&version_bytes);
        let version: u32 = version_str.parse().map_err(|_| format!("Invalid version: {}", version_str))?;

        if version > 12 {
            return Err(format!("Unsupported VDB version: {}", version));
        }

        if self.verbose {
            println!("[offset 5] Version: {}", version);
        }

        #[allow(unused_assignments)]
        let mut current_db: u16 = 0;

        // Parse entries
        loop {
            let offset = self.position;
            let opcode = self.read_byte()?;

            match opcode {
                VDB_OPCODE_AUX => {
                    let key = self.read_string()?;
                    let value = self.read_string()?;
                    if self.verbose {
                        println!("[offset {}] AUX: {} = {}", offset,
                                 String::from_utf8_lossy(&key),
                                 String::from_utf8_lossy(&value));
                    }
                }
                VDB_OPCODE_SELECTDB => {
                    current_db = self.read_length()? as u16;
                    if !self.databases.contains(&current_db) {
                        self.databases.push(current_db);
                    }
                    if self.verbose {
                        println!("[offset {}] Selecting DB {}", offset, current_db);
                    }
                }
                VDB_OPCODE_RESIZEDB => {
                    let db_size = self.read_length()?;
                    let expires_size = self.read_length()?;
                    if self.verbose {
                        println!("[offset {}] DB resize: {} keys, {} expires", offset, db_size, expires_size);
                    }
                }
                VDB_OPCODE_EXPIRETIME_MS => {
                    let _expire_ms = self.read_bytes(8)?;
                    self.expires += 1;
                }
                VDB_OPCODE_EXPIRETIME => {
                    let _expire_s = self.read_bytes(4)?;
                    self.expires += 1;
                }
                VDB_OPCODE_EOF => {
                    if self.verbose {
                        println!("[offset {}] EOF marker", offset);
                    }
                    break;
                }
                value_type => {
                    // This is a value type
                    let key = self.read_string()?;
                    self.skip_value(value_type)?;

                    self.total_keys += 1;
                    match value_type {
                        VDB_TYPE_STRING => self.string_keys += 1,
                        VDB_TYPE_LIST | VDB_TYPE_LIST_QUICKLIST => self.list_keys += 1,
                        VDB_TYPE_SET | VDB_TYPE_SET_INTSET => self.set_keys += 1,
                        VDB_TYPE_ZSET | VDB_TYPE_ZSET_2 | VDB_TYPE_ZSET_ZIPLIST => self.zset_keys += 1,
                        VDB_TYPE_HASH | VDB_TYPE_HASH_ZIPLIST | VDB_TYPE_HASH_ZIPMAP => self.hash_keys += 1,
                        _ => self.other_keys += 1,
                    }

                    if self.verbose {
                        println!("[offset {}] Key '{}' (type {})", offset,
                                 String::from_utf8_lossy(&key), value_type);
                    }
                }
            }
        }

        // Verify CRC64
        let computed_crc = self.crc64;

        // Read stored CRC without updating our computed CRC
        let mut stored_crc_bytes = [0u8; 8];
        self.reader.read_exact(&mut stored_crc_bytes)
            .map_err(|e| format!("Cannot read CRC64: {}", e))?;
        let stored_crc = u64::from_le_bytes(stored_crc_bytes);

        if stored_crc != 0 && stored_crc != computed_crc {
            return Err(format!("CRC64 mismatch: stored={:016x}, computed={:016x}", stored_crc, computed_crc));
        }

        if self.verbose {
            println!("[checksum] CRC64: {:016x} ({})", computed_crc,
                     if stored_crc == 0 { "not verified" } else { "OK" });
        }

        Ok(())
    }

    fn print_summary(&self) {
        println!("\n--- VDB Check Summary ---");
        println!("Total keys: {}", self.total_keys);
        println!("  Strings:     {}", self.string_keys);
        println!("  Lists:       {}", self.list_keys);
        println!("  Sets:        {}", self.set_keys);
        println!("  Sorted Sets: {}", self.zset_keys);
        println!("  Hashes:      {}", self.hash_keys);
        if self.other_keys > 0 {
            println!("  Other:       {}", self.other_keys);
        }
        println!("Keys with expiry: {}", self.expires);
        println!("Databases used: {:?}", self.databases);
    }
}

fn print_usage() {
    println!("Usage: viator-check-vdb [OPTIONS] <dump.vdb>

Check the integrity of a Viator VDB dump file.

Options:
  -v, --verbose    Show detailed information about each entry
  --help           Show this help message

Examples:
  viator-check-vdb dump.vdb
  viator-check-vdb -v /var/lib/viator/dump.vdb
");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut verbose = false;
    let mut file_path: Option<PathBuf> = None;

    for arg in &args[1..] {
        match arg.as_str() {
            "-v" | "--verbose" => verbose = true,
            "--help" | "-h" => {
                print_usage();
                return;
            }
            _ if !arg.starts_with('-') => {
                file_path = Some(PathBuf::from(arg));
            }
            _ => {
                eprintln!("Unknown option: {}", arg);
                std::process::exit(1);
            }
        }
    }

    let path = match file_path {
        Some(p) => p,
        None => {
            eprintln!("Error: No VDB file specified");
            print_usage();
            std::process::exit(1);
        }
    };

    if !path.exists() {
        eprintln!("Error: File not found: {}", path.display());
        std::process::exit(1);
    }

    // Get file size
    let file_size = std::fs::metadata(&path)
        .map(|m| m.len())
        .unwrap_or(0);

    println!("Checking VDB file: {}", path.display());
    println!("File size: {} bytes", file_size);

    let mut checker = match VdbChecker::new(&path, verbose) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    match checker.check() {
        Ok(()) => {
            checker.print_summary();
            println!("\n\x1b[32mVDB file is valid.\x1b[0m");
        }
        Err(e) => {
            eprintln!("\n\x1b[31mVDB file is CORRUPTED: {}\x1b[0m", e);
            std::process::exit(1);
        }
    }
}
