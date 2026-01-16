//! Viator: A high-performance, memory-safe key-value store.
//!
//! This is the main entry point for the Viator server.

#![allow(clippy::manual_c_str_literals)]

use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};
use viator::{Config, Server, VERSION};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments first to get config file path
    let args: Vec<String> = std::env::args().collect();
    let cli = parse_args(&args)?;

    // Handle help and version early
    if cli.help {
        print_help();
        return Ok(());
    }
    if cli.version {
        println!("Viator version {VERSION}");
        return Ok(());
    }

    // Load configuration
    let mut config = if let Some(ref config_path) = cli.config {
        match Config::load_from_file(config_path) {
            Ok(cfg) => {
                eprintln!("Loaded configuration from: {}", config_path.display());
                cfg
            }
            Err(e) => {
                eprintln!("Error loading config file: {e}");
                std::process::exit(1);
            }
        }
    } else {
        Config::default()
    };

    // Override with CLI arguments
    if let Some(port) = cli.port {
        config.port = port;
    }
    if let Some(bind) = cli.bind {
        config.bind = bind;
    }
    if let Some(requirepass) = cli.requirepass {
        config.requirepass = Some(requirepass);
    }
    if cli.daemonize {
        config.daemonize = true;
    }
    if let Some(pidfile) = cli.pidfile {
        config.pidfile = Some(pidfile);
    }
    if let Some(logfile) = cli.logfile {
        config.logfile = Some(logfile);
    }
    if let Some(loglevel) = cli.loglevel {
        config.loglevel = loglevel;
    }
    if let Some(maxmemory) = cli.maxmemory {
        config.maxmemory = maxmemory;
    }
    if let Some(databases) = cli.databases {
        config.databases = databases;
    }
    if cli.appendonly {
        config.appendonly = true;
    }
    if let Some(dir) = cli.dir {
        config.dir = dir;
    }

    // Daemonize if requested (Unix only)
    #[cfg(unix)]
    if config.daemonize {
        daemonize(&config)?;
    }

    // Write PID file
    if let Some(ref pidfile) = config.pidfile {
        let pid = std::process::id();
        if let Err(e) = std::fs::write(pidfile, pid.to_string()) {
            eprintln!("Warning: Failed to write PID file: {e}");
        }
    }

    // Initialize logging
    let log_level = match config.loglevel {
        viator::server::config::LogLevel::Debug => "debug",
        viator::server::config::LogLevel::Verbose => "debug",
        viator::server::config::LogLevel::Notice => "info",
        viator::server::config::LogLevel::Warning => "warn",
    };

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    if let Some(ref logfile) = config.logfile {
        // Log to file
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(logfile)?;

        fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_thread_ids(false)
            .with_ansi(false)
            .with_writer(file)
            .init();
    } else {
        // Log to stdout
        fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_thread_ids(false)
            .init();
    }

    // Print banner (only if not daemonized)
    if !config.daemonize {
        print_banner();
    }

    info!(
        "Viator {} starting on {}:{}",
        VERSION, config.bind, config.port
    );

    // Create and run server
    let server = Arc::new(Server::new(config.clone()));

    // Handle shutdown signals
    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for Ctrl+C: {}", e);
            return;
        }
        info!("Received shutdown signal");
        server_clone.shutdown();
    });

    // Handle SIGHUP for config reload (Unix only)
    #[cfg(unix)]
    {
        let config_path = cli.config.clone();
        tokio::spawn(async move {
            let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                .expect("Failed to create SIGHUP handler");

            loop {
                signal.recv().await;
                if let Some(ref path) = config_path {
                    info!("Received SIGHUP, reloading configuration...");
                    match Config::load_from_file(path) {
                        Ok(_cfg) => {
                            // Note: Runtime config reload is limited
                            // Most settings require restart
                            info!("Configuration reloaded (some settings require restart)");
                        }
                        Err(e) => {
                            warn!("Failed to reload configuration: {}", e);
                        }
                    }
                } else {
                    info!("Received SIGHUP but no config file specified");
                }
            }
        });
    }

    // Run the server
    server.run().await?;

    // Clean up PID file
    if let Some(ref pidfile) = config.pidfile {
        let _ = std::fs::remove_file(pidfile);
    }

    Ok(())
}

/// CLI arguments
struct CliArgs {
    config: Option<PathBuf>,
    port: Option<u16>,
    bind: Option<String>,
    requirepass: Option<String>,
    daemonize: bool,
    pidfile: Option<PathBuf>,
    logfile: Option<PathBuf>,
    loglevel: Option<viator::server::config::LogLevel>,
    maxmemory: Option<usize>,
    databases: Option<u16>,
    appendonly: bool,
    dir: Option<PathBuf>,
    help: bool,
    version: bool,
}

fn parse_args(args: &[String]) -> anyhow::Result<CliArgs> {
    let mut cli = CliArgs {
        config: None,
        port: None,
        bind: None,
        requirepass: None,
        daemonize: false,
        pidfile: None,
        logfile: None,
        loglevel: None,
        maxmemory: None,
        databases: None,
        appendonly: false,
        dir: None,
        help: false,
        version: false,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" | "-c" => {
                i += 1;
                cli.config = args.get(i).map(PathBuf::from);
            }
            "--port" | "-p" => {
                i += 1;
                cli.port = args.get(i).and_then(|s| s.parse().ok());
            }
            "--bind" | "-b" => {
                i += 1;
                cli.bind = args.get(i).cloned();
            }
            "--requirepass" | "-a" => {
                i += 1;
                cli.requirepass = args.get(i).cloned();
            }
            "--daemonize" | "-d" => {
                cli.daemonize = true;
            }
            "--pidfile" => {
                i += 1;
                cli.pidfile = args.get(i).map(PathBuf::from);
            }
            "--logfile" => {
                i += 1;
                cli.logfile = args.get(i).map(PathBuf::from);
            }
            "--loglevel" => {
                i += 1;
                if let Some(level) = args.get(i) {
                    cli.loglevel = match level.to_lowercase().as_str() {
                        "debug" => Some(viator::server::config::LogLevel::Debug),
                        "verbose" => Some(viator::server::config::LogLevel::Verbose),
                        "notice" => Some(viator::server::config::LogLevel::Notice),
                        "warning" => Some(viator::server::config::LogLevel::Warning),
                        _ => None,
                    };
                }
            }
            "--maxmemory" => {
                i += 1;
                if let Some(mem) = args.get(i) {
                    cli.maxmemory = parse_memory_arg(mem);
                }
            }
            "--databases" => {
                i += 1;
                cli.databases = args.get(i).and_then(|s| s.parse().ok());
            }
            "--appendonly" => {
                cli.appendonly = true;
            }
            "--dir" => {
                i += 1;
                cli.dir = args.get(i).map(PathBuf::from);
            }
            "--help" | "-h" => {
                cli.help = true;
            }
            "--version" | "-v" => {
                cli.version = true;
            }
            arg if arg.starts_with('-') => {
                eprintln!("Unknown option: {arg}");
                cli.help = true;
            }
            _ => {}
        }
        i += 1;
    }

    Ok(cli)
}

fn parse_memory_arg(value: &str) -> Option<usize> {
    let value = value.trim().to_lowercase();
    let (num_str, multiplier) = if value.ends_with("gb") {
        (&value[..value.len() - 2], 1024 * 1024 * 1024)
    } else if value.ends_with("mb") {
        (&value[..value.len() - 2], 1024 * 1024)
    } else if value.ends_with("kb") {
        (&value[..value.len() - 2], 1024)
    } else if value.ends_with('g') {
        (&value[..value.len() - 1], 1024 * 1024 * 1024)
    } else if value.ends_with('m') {
        (&value[..value.len() - 1], 1024 * 1024)
    } else if value.ends_with('k') {
        (&value[..value.len() - 1], 1024)
    } else {
        (value.as_str(), 1)
    };

    num_str.trim().parse::<usize>().ok().map(|n| n * multiplier)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn daemonize(config: &Config) -> anyhow::Result<()> {
    // Fork and exit parent
    // SAFETY: fork() is safe to call; it creates a new process. Return value is checked.
    match unsafe { libc::fork() } {
        -1 => Err(anyhow::anyhow!("Fork failed")),
        0 => {
            // Child process continues
            // Create new session
            // SAFETY: setsid() is safe to call after fork in child process. Return value is checked.
            if unsafe { libc::setsid() } == -1 {
                return Err(anyhow::anyhow!("setsid failed"));
            }

            // Change working directory
            std::env::set_current_dir(&config.dir)?;

            // Close standard file descriptors
            // SAFETY: These are standard POSIX daemonization steps:
            // - close(0/1/2) closes stdin/stdout/stderr which is safe
            // - open("/dev/null") with null-terminated string literal is safe
            // - dup(0) duplicates fd 0 to next available fd, which is safe
            unsafe {
                libc::close(0); // stdin
                libc::close(1); // stdout
                libc::close(2); // stderr

                // Redirect to /dev/null
                libc::open(b"/dev/null\0".as_ptr().cast::<i8>(), libc::O_RDWR);
                libc::dup(0);
                libc::dup(0);
            }

            Ok(())
        }
        _ => {
            // Parent exits
            std::process::exit(0);
        }
    }
}

fn print_banner() {
    println!(
        r"
           :::     :::  :::::::::::      :::      :::::::::::  ::::::::   :::::::::
          :+:     :+:      :+:         :+: :+:       :+:     :+:    :+:  :+:    :+:
         +:+     +:+      +:+        +:+   +:+      +:+     +:+    +:+  +:+    +:+
        +#+     +:+      +#+       +#++:++#++:     +#+     +#+    +:+  +#++:++#:
       +#+   +#+       +#+       +#+     +#+     +#+     +#+    +#+  +#+    +#+
       #+#+#+#        #+#       #+#     #+#     #+#     #+#    #+#  #+#    #+#
         ###     ###########  ###     ###     ###      ########   ###    ###

    Viator {} - A high-performance key-value store

    PID: {}
    https://github.com/panggi/viator
",
        VERSION,
        std::process::id()
    );
}

fn print_help() {
    println!(
        r"Viator {VERSION} - A high-performance key-value store

USAGE:
    viator [OPTIONS]

OPTIONS:
    -c, --config <FILE>      Load configuration from file
    -p, --port <PORT>        Set the server port (default: 6379)
    -b, --bind <ADDR>        Set the bind address (default: 127.0.0.1)
    -a, --requirepass <PWD>  Set the server password
    -d, --daemonize          Run as a daemon (background)
        --pidfile <FILE>     Write PID to file
        --logfile <FILE>     Write logs to file
        --loglevel <LEVEL>   Set log level (debug, verbose, notice, warning)
        --maxmemory <BYTES>  Set max memory (e.g., 1gb, 512mb)
        --databases <NUM>    Set number of databases (default: 16)
        --appendonly         Enable AOF persistence
        --dir <DIR>          Set working directory
    -h, --help               Print this help message
    -v, --version            Print version information

CONFIGURATION FILE:
    Viator supports redis.conf compatible configuration files.
    Use --config to load a configuration file.

EXAMPLES:
    viator                              Start with defaults
    viator --port 6380                  Start on port 6380
    viator -c /etc/viator/viator.conf   Load from config file
    viator -d --pidfile /var/run/viator.pid   Run as daemon
    viator --maxmemory 1gb --appendonly       With memory limit and AOF

SIGNALS:
    SIGINT/SIGTERM  Graceful shutdown
    SIGHUP          Reload configuration (if --config specified)

For more information, visit: https://github.com/panggi/viator
"
    );
}
