//! Viator Check AOF - AOF file validation and repair tool
//!
//! A tool for checking and optionally repairing Viator AOF files.
//! Validates RESP protocol format and can truncate corrupted entries.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

/// AOF file checker and repairer
struct AofChecker {
    path: PathBuf,
    reader: BufReader<File>,
    position: u64,
    verbose: bool,
    // Statistics
    total_commands: u64,
    valid_commands: u64,
    last_valid_position: u64,
}

impl AofChecker {
    fn new(path: &PathBuf, verbose: bool) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("Cannot open file: {e}"))?;
        Ok(Self {
            path: path.clone(),
            reader: BufReader::new(file),
            position: 0,
            verbose,
            total_commands: 0,
            valid_commands: 0,
            last_valid_position: 0,
        })
    }

    fn read_line(&mut self) -> Result<String, String> {
        let mut line = String::new();
        let bytes_read = self
            .reader
            .read_line(&mut line)
            .map_err(|e| format!("Read error at position {}: {}", self.position, e))?;

        if bytes_read == 0 {
            return Err("EOF".to_string());
        }

        self.position += bytes_read as u64;

        // Remove trailing \r\n or \n
        if line.ends_with('\n') {
            line.pop();
        }
        if line.ends_with('\r') {
            line.pop();
        }

        Ok(line)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, String> {
        let mut buf = vec![0u8; len];
        self.reader
            .read_exact(&mut buf)
            .map_err(|e| format!("Read error at position {}: {}", self.position, e))?;
        self.position += len as u64;
        Ok(buf)
    }

    fn skip_crlf(&mut self) -> Result<(), String> {
        let mut crlf = [0u8; 2];
        self.reader
            .read_exact(&mut crlf)
            .map_err(|e| format!("Expected CRLF at position {}: {}", self.position, e))?;

        if crlf != [b'\r', b'\n'] {
            return Err(format!(
                "Expected CRLF at position {}, got {:?}",
                self.position, crlf
            ));
        }

        self.position += 2;
        Ok(())
    }

    fn parse_command(&mut self) -> Result<Vec<Vec<u8>>, String> {
        let line = self.read_line()?;

        if line.is_empty() {
            return Err("Empty line".to_string());
        }

        // Must start with *
        if !line.starts_with('*') {
            // Could be inline command
            if line.starts_with(|c: char| c.is_ascii_alphabetic()) {
                let parts: Vec<Vec<u8>> = line
                    .split_whitespace()
                    .map(|s| s.as_bytes().to_vec())
                    .collect();
                return Ok(parts);
            }
            return Err(format!(
                "Expected '*' at position {}, got '{}'",
                self.position,
                &line[..1.min(line.len())]
            ));
        }

        // Parse array length
        let num_args: usize = line[1..].parse().map_err(|_| {
            format!(
                "Invalid array length '{}' at position {}",
                &line[1..],
                self.position
            )
        })?;

        if num_args == 0 || num_args > 1024 * 1024 {
            return Err(format!(
                "Invalid number of arguments {} at position {}",
                num_args, self.position
            ));
        }

        let mut args = Vec::with_capacity(num_args);

        for i in 0..num_args {
            let bulk_line = self.read_line()?;

            if !bulk_line.starts_with('$') {
                return Err(format!(
                    "Expected '$' for argument {} at position {}",
                    i, self.position
                ));
            }

            let len: usize = bulk_line[1..].parse().map_err(|_| {
                format!(
                    "Invalid bulk length '{}' at position {}",
                    &bulk_line[1..],
                    self.position
                )
            })?;

            if len > 512 * 1024 * 1024 {
                return Err(format!(
                    "Bulk string too large ({} bytes) at position {}",
                    len, self.position
                ));
            }

            let data = self.read_bytes(len)?;
            self.skip_crlf()?;

            args.push(data);
        }

        Ok(args)
    }

    fn check(&mut self) -> Result<bool, String> {
        println!("Start checking Old-Style AOF");
        println!("[offset 0] Checking AOF file {}", self.path.display());

        let mut is_valid = true;
        let mut first_error: Option<String> = None;

        loop {
            let cmd_start = self.position;

            match self.parse_command() {
                Ok(args) => {
                    self.total_commands += 1;
                    self.valid_commands += 1;
                    self.last_valid_position = self.position;

                    if self.verbose && !args.is_empty() {
                        let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
                        println!(
                            "[offset {}] {} with {} args",
                            cmd_start,
                            cmd_name,
                            args.len()
                        );
                    }
                }
                Err(e) if e == "EOF" => {
                    break;
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(format!("0x{:x}: {}", cmd_start, e));
                    }
                    is_valid = false;

                    // Try to find next valid command
                    if self.try_recover() {
                        self.total_commands += 1;
                        continue;
                    }
                    break;
                }
            }
        }

        println!(
            "AOF analyzed: size={}, ok_up_to={}, ok_up_to_line={}, diff={}",
            self.position,
            self.last_valid_position,
            self.valid_commands,
            self.position - self.last_valid_position
        );

        if let Some(err) = &first_error {
            println!("{err}");
        }

        Ok(is_valid)
    }

    fn try_recover(&mut self) -> bool {
        // Try to find the next valid RESP array marker
        let mut buf = [0u8; 1];
        let mut found_star = false;

        for _ in 0..10000 {
            if self.reader.read_exact(&mut buf).is_err() {
                return false;
            }
            self.position += 1;

            if buf[0] == b'*' {
                found_star = true;
                // Back up one byte to include the *
                if self.reader.seek(SeekFrom::Current(-1)).is_ok() {
                    self.position -= 1;
                }
                break;
            }
        }

        found_star
    }

    fn truncate_at_last_valid(&self) -> Result<(), String> {
        if self.last_valid_position == 0 {
            return Err("No valid commands found, cannot truncate".to_string());
        }

        let file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .map_err(|e| format!("Cannot open file for writing: {e}"))?;

        file.set_len(self.last_valid_position)
            .map_err(|e| format!("Cannot truncate file: {e}"))?;

        println!("Truncated file to {} bytes", self.last_valid_position);
        Ok(())
    }
}

fn print_usage() {
    println!(
        "Usage: viator-check-aof [OPTIONS] <appendonly.aof>

Check the integrity of a Viator/Redis AOF file and optionally repair it.

Options:
  -v, --verbose    Show detailed information about each command
  --fix            Truncate the AOF file at the last valid command
  --help           Show this help message

Examples:
  viator-check-aof appendonly.aof
  viator-check-aof -v /var/lib/viator/appendonly.aof
  viator-check-aof --fix appendonly.aof
"
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut verbose = false;
    let mut fix = false;
    let mut file_path: Option<PathBuf> = None;

    for arg in &args[1..] {
        match arg.as_str() {
            "-v" | "--verbose" => verbose = true,
            "--fix" => fix = true,
            "--help" | "-h" => {
                print_usage();
                return;
            }
            _ if !arg.starts_with('-') => {
                file_path = Some(PathBuf::from(arg));
            }
            _ => {
                eprintln!("Unknown option: {arg}");
                std::process::exit(1);
            }
        }
    }

    let path = if let Some(p) = file_path {
        p
    } else {
        eprintln!("Error: No AOF file specified");
        print_usage();
        std::process::exit(1);
    };

    if !path.exists() {
        eprintln!("Error: File not found: {}", path.display());
        std::process::exit(1);
    }

    let mut checker = match AofChecker::new(&path, verbose) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    match checker.check() {
        Ok(true) => {
            println!("AOF {} is valid", path.display());
        }
        Ok(false) => {
            eprintln!(
                "AOF {} is not valid. Use the --fix option to try fixing it.",
                path.display()
            );

            if fix {
                println!("Trying to fix by truncating at last valid command...");
                match checker.truncate_at_last_valid() {
                    Ok(()) => {
                        println!("Successfully truncated AOF {}", path.display());
                    }
                    Err(e) => {
                        eprintln!("Failed to fix AOF: {e}");
                        std::process::exit(1);
                    }
                }
            } else {
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Error checking AOF: {e}");
            std::process::exit(1);
        }
    }
}
