//! Command executor - the main entry point for command processing.

use super::ParsedCommand;
use super::registry::{Command, CommandRegistry};
use crate::Result;
use crate::error::{CommandError, Error};
use crate::protocol::Frame;
use crate::server::cluster::{ClusterManager, SlotCheck, key_slot};
use crate::server::{ClientState, SharedPubSubHub};
use crate::storage::Database;
use std::sync::Arc;
use tracing::{debug, trace};

/// Commands that are allowed during a transaction (not queued).
const TRANSACTION_COMMANDS: &[&str] = &["EXEC", "DISCARD", "MULTI", "WATCH", "UNWATCH"];

/// Commands that don't require slot checking (node-local commands).
const SKIP_SLOT_CHECK: &[&str] = &[
    // Cluster commands
    "CLUSTER",
    "ASKING",
    "READONLY",
    "READWRITE",
    // Connection commands
    "PING",
    "ECHO",
    "AUTH",
    "HELLO",
    "CLIENT",
    "SELECT",
    "QUIT",
    "RESET",
    // Server commands
    "INFO",
    "DEBUG",
    "CONFIG",
    "COMMAND",
    "MEMORY",
    "MODULE",
    "SLOWLOG",
    "ACL",
    "LATENCY",
    "DBSIZE",
    "LASTSAVE",
    "TIME",
    "BGSAVE",
    "BGREWRITEAOF",
    "SAVE",
    "SHUTDOWN",
    "FLUSHALL",
    "FLUSHDB",
    // Transaction commands
    "MULTI",
    "EXEC",
    "DISCARD",
    "WATCH",
    "UNWATCH",
    // Pub/Sub commands
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "PUBLISH",
    "PUBSUB",
    "SSUBSCRIBE",
    "SUNSUBSCRIBE",
    "SPUBLISH",
    // Script commands
    "EVAL",
    "EVALSHA",
    "SCRIPT",
    "FUNCTION",
    "FCALL",
    "FCALL_RO",
    // Sentinel commands
    "SENTINEL",
    // Keys without specific slot (admin commands)
    "KEYS",
    "SCAN",
    "OBJECT",
    "WAIT",
    "WAITAOF",
    "DEBUG",
];

/// Command executor responsible for routing and executing Redis commands.
#[derive(Debug)]
pub struct CommandExecutor {
    /// Command registry
    registry: CommandRegistry,
    /// Database
    database: Arc<Database>,
    /// Pub/Sub hub (optional, set after construction)
    pubsub: Option<SharedPubSubHub>,
    /// Cluster manager (optional, for cluster mode)
    cluster: Option<Arc<ClusterManager>>,
}

impl CommandExecutor {
    /// Create a new command executor.
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            registry: CommandRegistry::new(),
            database,
            pubsub: None,
            cluster: None,
        }
    }

    /// Create a new command executor with Pub/Sub support.
    pub fn with_pubsub(database: Arc<Database>, pubsub: SharedPubSubHub) -> Self {
        Self {
            registry: CommandRegistry::new(),
            database,
            pubsub: Some(pubsub),
            cluster: None,
        }
    }

    /// Create a new command executor with cluster support.
    pub fn with_cluster(
        database: Arc<Database>,
        pubsub: SharedPubSubHub,
        cluster: Arc<ClusterManager>,
    ) -> Self {
        Self {
            registry: CommandRegistry::new(),
            database,
            pubsub: Some(pubsub),
            cluster: Some(cluster),
        }
    }

    /// Get the Pub/Sub hub.
    pub fn pubsub(&self) -> Option<&SharedPubSubHub> {
        self.pubsub.as_ref()
    }

    /// Get the cluster manager.
    pub fn cluster(&self) -> Option<&Arc<ClusterManager>> {
        self.cluster.as_ref()
    }

    /// Execute a command.
    pub async fn execute(&self, cmd: ParsedCommand, client: Arc<ClientState>) -> Result<Frame> {
        trace!(
            "Executing command: {} with {} args",
            cmd.name,
            cmd.arg_count()
        );

        let cmd_upper = cmd.name.to_uppercase();

        // Handle PUBLISH command specially (needs access to PubSubHub)
        if cmd_upper == "PUBLISH" {
            return self.handle_publish(&cmd).await;
        }

        // Handle PUBSUB command specially
        if cmd_upper == "PUBSUB" {
            return self.handle_pubsub_command(&cmd).await;
        }

        // Handle AUTH command specially (needs access to server auth config)
        if cmd_upper == "AUTH" {
            return self.handle_auth(&cmd, client).await;
        }

        // Handle HELLO command specially (needs access to server auth config)
        if cmd_upper == "HELLO" {
            return self.handle_hello(&cmd, client).await;
        }

        // Handle INFO command specially (needs access to Database for connected_clients)
        if cmd_upper == "INFO" {
            return self.handle_info(&cmd).await;
        }

        // Handle SHUTDOWN command specially (needs access to Database for shutdown flag)
        if cmd_upper == "SHUTDOWN" {
            return self.handle_shutdown(&cmd).await;
        }

        // Look up command
        let command = self
            .registry
            .get(&cmd.name)
            .ok_or_else(|| Error::Command(CommandError::UnknownCommand(cmd.name.clone())))?;

        // Check argument count
        let argc = cmd.arg_count() as i32;
        if argc < command.min_args {
            return Err(CommandError::WrongArity {
                command: cmd.name.clone(),
            }
            .into());
        }
        if command.max_args >= 0 && argc > command.max_args {
            return Err(CommandError::WrongArity {
                command: cmd.name.clone(),
            }
            .into());
        }

        // Check cluster slot ownership for commands that access keys
        if let Some(cluster) = &self.cluster {
            if cluster.is_enabled() && !SKIP_SLOT_CHECK.contains(&cmd_upper.as_str()) {
                // Check if client sent ASKING (for ASK redirections)
                let is_asking = client.is_asking();
                if is_asking {
                    client.clear_asking();
                }

                // Get the first key from the command (key position is typically 1 for most commands)
                if let Some(key) = self.get_first_key(&cmd, command) {
                    let slot = key_slot(&key);
                    match cluster.check_slot_number(slot) {
                        SlotCheck::Owned => {
                            // We own this slot, proceed with execution
                        }
                        SlotCheck::Moved { slot, addr } => {
                            // Permanent redirect - client should update its slot table
                            return Ok(Frame::Error(format!(
                                "MOVED {} {}:{}",
                                slot,
                                addr.ip(),
                                addr.port()
                            )));
                        }
                        SlotCheck::Ask { slot, addr } => {
                            // Migration in progress - client should send ASKING first
                            if !is_asking {
                                return Ok(Frame::Error(format!(
                                    "ASK {} {}:{}",
                                    slot,
                                    addr.ip(),
                                    addr.port()
                                )));
                            }
                            // Client sent ASKING, let them access the key during migration
                        }
                        SlotCheck::ClusterDown => {
                            return Ok(Frame::Error("CLUSTERDOWN The cluster is down".to_string()));
                        }
                    }
                }
            }
        }

        // Handle transaction mode
        if client.is_in_transaction() {
            // EXEC - execute all queued commands
            if cmd_upper == "EXEC" {
                return self.execute_transaction(client).await;
            }

            // These commands are executed immediately, not queued
            if !TRANSACTION_COMMANDS.contains(&cmd_upper.as_str()) {
                // Queue the command for later execution
                client.queue_command(cmd.name.clone(), cmd.args.clone());
                return Ok(Frame::queued());
            }
        }

        // Get the appropriate database
        let db = self.database.get_db(client.db_index())?;

        // Execute the command
        let result = (command.handler)(cmd, db, client).await;

        match &result {
            Ok(_) => trace!("Command executed successfully"),
            Err(e) => debug!("Command failed: {}", e),
        }

        result
    }

    /// Execute all queued commands in a transaction.
    async fn execute_transaction(&self, client: Arc<ClientState>) -> Result<Frame> {
        // Check if transaction was aborted due to WATCH
        if client.is_transaction_aborted() {
            client.discard_transaction();
            return Ok(Frame::Null);
        }

        // Take queued commands
        let commands = client.take_queued_commands();

        // Clear transaction state
        client.set_in_transaction(false);
        client.unwatch_all();

        // Execute each command and collect results
        let mut results = Vec::with_capacity(commands.len());

        for queued in commands {
            // Reconstruct the ParsedCommand
            let cmd = ParsedCommand {
                name: queued.name,
                args: queued.args,
            };

            // Look up and execute the command
            match self.registry.get(&cmd.name) {
                Some(command) => {
                    let db = match self.database.get_db(client.db_index()) {
                        Ok(db) => db,
                        Err(e) => {
                            results.push(Frame::error(e.to_viator_error()));
                            continue;
                        }
                    };

                    match (command.handler)(cmd, db, Arc::clone(&client)).await {
                        Ok(frame) => results.push(frame),
                        Err(e) => results.push(Frame::error(e.to_viator_error())),
                    }
                }
                None => {
                    results.push(Frame::error(format!("ERR unknown command '{}'", cmd.name)));
                }
            }
        }

        Ok(Frame::Array(results))
    }

    /// Get the command registry.
    pub fn registry(&self) -> &CommandRegistry {
        &self.registry
    }

    /// Get the database.
    pub fn database(&self) -> &Arc<Database> {
        &self.database
    }

    /// Handle PUBLISH command.
    async fn handle_publish(&self, cmd: &ParsedCommand) -> Result<Frame> {
        if cmd.args.len() < 2 {
            return Err(CommandError::WrongArity {
                command: "PUBLISH".to_string(),
            }
            .into());
        }

        let pubsub = self
            .pubsub
            .as_ref()
            .ok_or_else(|| Error::Internal("Pub/Sub not initialized".to_string()))?;

        let channel = cmd.args[0].clone();
        let message = cmd.args[1].clone();
        let count = pubsub.publish(channel, message);

        Ok(Frame::Integer(count as i64))
    }

    /// Handle PUBSUB command.
    async fn handle_pubsub_command(&self, cmd: &ParsedCommand) -> Result<Frame> {
        if cmd.args.is_empty() {
            return Err(CommandError::WrongArity {
                command: "PUBSUB".to_string(),
            }
            .into());
        }

        let pubsub = self
            .pubsub
            .as_ref()
            .ok_or_else(|| Error::Internal("Pub/Sub not initialized".to_string()))?;

        let subcommand = cmd.get_str(0)?.to_uppercase();
        match subcommand.as_str() {
            "CHANNELS" => {
                let pattern = cmd.args.get(1).cloned();
                let channels = pubsub.channels(pattern.as_ref());
                let frames: Vec<Frame> = channels.into_iter().map(Frame::Bulk).collect();
                Ok(Frame::Array(frames))
            }
            "NUMSUB" => {
                let mut results = Vec::new();
                for channel in cmd.args.iter().skip(1) {
                    results.push(Frame::Bulk(channel.clone()));
                    results.push(Frame::Integer(pubsub.numsub(channel) as i64));
                }
                Ok(Frame::Array(results))
            }
            "NUMPAT" => Ok(Frame::Integer(pubsub.numpat() as i64)),
            _ => Err(CommandError::UnknownCommand(format!("PUBSUB {}", subcommand)).into()),
        }
    }

    /// Handle AUTH command with actual password validation.
    async fn handle_auth(&self, cmd: &ParsedCommand, client: Arc<ClientState>) -> Result<Frame> {
        // Check if client is already locked out
        if let Some(secs) = client.auth_is_blocked() {
            return Ok(Frame::Error(format!(
                "ERR too many authentication failures, account locked for {} seconds",
                secs
            )));
        }

        // Validate argument count (1 for legacy AUTH, 2 for AUTH username password)
        if cmd.args.is_empty() || cmd.args.len() > 2 {
            return Err(CommandError::WrongArity {
                command: "AUTH".to_string(),
            }
            .into());
        }

        // Parse credentials - get the password (ignore username for now)
        let password = if cmd.args.len() >= 2 {
            // AUTH username password format (Redis 6+)
            cmd.get_str(1)?
        } else {
            // AUTH password format (legacy)
            cmd.get_str(0)?
        };

        // Validate password against server config
        let server_auth = self.database.server_auth();
        let password_valid = server_auth.validate_password(password);

        if password_valid {
            // Success - reset failure counter and mark authenticated
            client.auth_reset_failures();
            client.set_authenticated(true);
            Ok(Frame::ok())
        } else {
            // Failure - record attempt and check if locked out
            let locked_out = client.auth_record_failure();
            if locked_out {
                Ok(Frame::Error(
                    "ERR too many authentication failures, account locked for 60 seconds"
                        .to_string(),
                ))
            } else {
                Ok(Frame::Error(
                    "WRONGPASS invalid username-password pair".to_string(),
                ))
            }
        }
    }

    /// Handle HELLO command with password validation.
    async fn handle_hello(&self, cmd: &ParsedCommand, client: Arc<ClientState>) -> Result<Frame> {
        let proto_version = if !cmd.args.is_empty() {
            cmd.get_u64(0)? as u8
        } else {
            2 // Default to RESP2
        };

        // Parse optional AUTH and SETNAME
        let mut i = 1;
        let mut auth_error: Option<Frame> = None;

        while i < cmd.args.len() {
            let opt = cmd.get_str(i)?.to_uppercase();
            match opt.as_str() {
                "AUTH" => {
                    // AUTH username password
                    if i + 2 >= cmd.args.len() {
                        return Ok(Frame::Error(
                            "ERR Syntax error in HELLO option 'AUTH'".to_string(),
                        ));
                    }
                    let _username = cmd.get_str(i + 1)?;
                    let password = cmd.get_str(i + 2)?;

                    // Validate password
                    let server_auth = self.database.server_auth();
                    if server_auth.validate_password(password) {
                        client.set_authenticated(true);
                    } else {
                        // Store error but continue parsing to set up response
                        auth_error = Some(Frame::Error(
                            "WRONGPASS invalid username-password pair".to_string(),
                        ));
                    }
                    i += 3;
                }
                "SETNAME" => {
                    i += 1;
                    if i < cmd.args.len() {
                        let name = cmd.get_str(i)?;
                        client.set_name(name.to_string());
                    }
                    i += 1;
                }
                _ => i += 1,
            }
        }

        // If AUTH failed, return the error
        if let Some(err) = auth_error {
            return Ok(err);
        }

        // Build response with server info
        let response = vec![
            Frame::bulk("server"),
            Frame::bulk("viator"),
            Frame::bulk("version"),
            Frame::bulk(crate::REDIS_VERSION),
            Frame::bulk("proto"),
            Frame::Integer(proto_version as i64),
            Frame::bulk("id"),
            Frame::Integer(client.id() as i64),
            Frame::bulk("mode"),
            Frame::bulk("standalone"),
            Frame::bulk("role"),
            Frame::bulk("master"),
            Frame::bulk("modules"),
            Frame::Array(vec![]),
        ];

        Ok(Frame::Array(response))
    }

    /// Handle INFO command with access to Database for accurate metrics.
    async fn handle_info(&self, cmd: &ParsedCommand) -> Result<Frame> {
        use crate::server::metrics::{format_bytes, get_memory_usage};

        let section = cmd.args.first().and_then(|s| std::str::from_utf8(s).ok());
        let mut info = String::new();
        let include_all = section.is_none() || section == Some("all");

        if include_all || section == Some("server") {
            info.push_str("# Server\r\n");
            info.push_str(&format!("redis_version:{}\r\n", crate::REDIS_VERSION));
            info.push_str(&format!("viator_version:{}\r\n", crate::VERSION));
            info.push_str("viator_mode:standalone\r\n");
            info.push_str(&format!("os:{}\r\n", std::env::consts::OS));
            info.push_str(&format!("arch_bits:{}\r\n", (usize::BITS as usize)));
            info.push_str("tcp_port:6379\r\n");
            info.push_str("\r\n");
        }

        if include_all || section == Some("clients") {
            info.push_str("# Clients\r\n");
            info.push_str(&format!(
                "connected_clients:{}\r\n",
                self.database.connected_clients()
            ));
            info.push_str(&format!(
                "total_connections_received:{}\r\n",
                self.database.total_connections()
            ));
            info.push_str("\r\n");
        }

        if include_all || section == Some("memory") {
            let (used_memory, used_memory_rss) = get_memory_usage();
            info.push_str("# Memory\r\n");
            info.push_str(&format!("used_memory:{}\r\n", used_memory));
            info.push_str(&format!(
                "used_memory_human:{}\r\n",
                format_bytes(used_memory)
            ));
            info.push_str(&format!("used_memory_rss:{}\r\n", used_memory_rss));
            info.push_str(&format!(
                "used_memory_rss_human:{}\r\n",
                format_bytes(used_memory_rss)
            ));
            let maxmemory = self.database.memory_manager().maxmemory();
            info.push_str(&format!("maxmemory:{}\r\n", maxmemory));
            info.push_str(&format!("maxmemory_human:{}\r\n", format_bytes(maxmemory)));
            info.push_str(&format!(
                "maxmemory_policy:{:?}\r\n",
                self.database.memory_manager().policy()
            ));
            info.push_str("\r\n");
        }

        if include_all || section == Some("stats") {
            info.push_str("# Stats\r\n");
            info.push_str(&format!(
                "total_connections_received:{}\r\n",
                self.database.total_connections()
            ));
            info.push_str(&format!(
                "evicted_keys:{}\r\n",
                self.database.memory_manager().evicted_keys()
            ));
            info.push_str("\r\n");
        }

        if include_all || section == Some("keyspace") {
            info.push_str("# Keyspace\r\n");
            let total_keys = self.database.total_keys();
            if total_keys > 0 {
                info.push_str(&format!("db0:keys={},expires=0\r\n", total_keys));
            }
            info.push_str("\r\n");
        }

        Ok(Frame::Bulk(info.into()))
    }

    /// Handle SHUTDOWN command.
    async fn handle_shutdown(&self, cmd: &ParsedCommand) -> Result<Frame> {
        let mut save = true; // Default: save before shutdown
        let mut abort = false;

        // Parse options: NOSAVE, SAVE, NOW, FORCE, ABORT
        for arg in &cmd.args {
            let opt = std::str::from_utf8(arg).unwrap_or("").to_uppercase();
            match opt.as_str() {
                "NOSAVE" => save = false,
                "SAVE" => save = true,
                "ABORT" => abort = true,
                "NOW" | "FORCE" => {} // Accepted but ignored (always immediate)
                _ => {}
            }
        }

        if abort {
            // Cancel pending shutdown
            self.database.cancel_shutdown();
            return Ok(Frame::ok());
        }

        // Request shutdown
        self.database.request_shutdown(save);

        // Return OK - the server will detect the shutdown request
        // and perform graceful shutdown
        Ok(Frame::ok())
    }

    /// Get the first key from a command for slot calculation.
    /// Returns None for commands that don't have keys.
    fn get_first_key(&self, cmd: &ParsedCommand, command: &Command) -> Option<bytes::Bytes> {
        // Commands marked as no_keys don't have keys
        if command.flags.no_keys() {
            return None;
        }

        // Most Redis commands have the key as the first argument
        // Some exceptions handled below
        let cmd_upper = cmd.name.to_uppercase();

        // Handle special commands with different key positions
        match cmd_upper.as_str() {
            // Commands with no keys (handled by SKIP_SLOT_CHECK but double check)
            "INFO" | "DEBUG" | "CONFIG" | "CLIENT" | "PING" | "ECHO" | "AUTH" | "HELLO" => None,

            // EVAL/EVALSHA: key starts at position 3 (after script, numkeys)
            "EVAL" | "EVALSHA" => {
                if cmd.args.len() >= 3 {
                    // numkeys is at position 1, first key at position 2
                    cmd.args.get(2).cloned()
                } else {
                    None
                }
            }

            // MIGRATE: key is at position 3 (host, port, key, ...)
            "MIGRATE" => cmd.args.get(2).cloned(),

            // OBJECT: subcommand then key
            "OBJECT" => cmd.args.get(1).cloned(),

            // SORT: key then options
            // DUMP, RESTORE, etc.: standard first arg

            // Default: first argument is the key
            _ => cmd.args.first().cloned(),
        }
    }
}
