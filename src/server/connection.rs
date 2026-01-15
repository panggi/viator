//! Connection handling for individual clients.

use super::metrics::ServerMetrics;
use super::pubsub::PubSubMessage;
use super::ClientState;
use crate::commands::{CommandExecutor, ParsedCommand};
use crate::protocol::{Frame, RespParser};
use crate::storage::Database;
use crate::Result;
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tracing::{debug, trace};

/// Buffer size for reading from socket.
const READ_BUFFER_SIZE: usize = 8 * 1024;

/// Maximum number of responses to batch before flushing.
/// This significantly improves throughput for pipelined requests.
const WRITE_BATCH_SIZE: usize = 64;

/// Maximum bytes to buffer before forcing a flush.
const WRITE_BUFFER_HIGH_WATER: usize = 64 * 1024;

/// A connection to a single client.
pub struct Connection {
    /// TCP stream
    stream: BufWriter<TcpStream>,
    /// Peer address
    peer_addr: SocketAddr,
    /// RESP parser
    parser: RespParser,
    /// Client state
    state: Arc<ClientState>,
    /// Command executor
    executor: Arc<CommandExecutor>,
    /// Database reference for per-connection DB selection (SELECT command)
    #[allow(dead_code)]
    database: Arc<Database>,
    /// Server metrics for performance tracking
    metrics: Arc<ServerMetrics>,
    /// Write buffer
    write_buffer: BytesMut,
    /// Number of pending writes (for batching)
    pending_writes: usize,
}

impl Connection {
    /// Create a new connection.
    pub fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        id: u64,
        executor: Arc<CommandExecutor>,
        database: Arc<Database>,
        metrics: Arc<ServerMetrics>,
    ) -> Self {
        debug!("New connection from {} (id={})", peer_addr, id);

        Self {
            stream: BufWriter::new(stream),
            peer_addr,
            parser: RespParser::new(),
            state: Arc::new(ClientState::new(id)),
            executor,
            database,
            metrics,
            write_buffer: BytesMut::with_capacity(4096),
            pending_writes: 0,
        }
    }

    /// Run the connection handler.
    pub async fn run(&mut self) -> Result<()> {
        let mut read_buf = vec![0u8; READ_BUFFER_SIZE];

        loop {
            // Check if connection should close
            if self.state.is_closed() {
                break;
            }

            // Check if we're in pub/sub mode
            if self.state.is_in_pubsub_mode() {
                self.run_pubsub_mode(&mut read_buf).await?;
                continue;
            }

            // Normal command mode
            // Read data from socket
            let n = self.stream.get_mut().read(&mut read_buf).await?;
            if n == 0 {
                // Connection closed by peer
                debug!("Connection closed by peer: {}", self.peer_addr);
                break;
            }

            trace!("Read {} bytes from {}", n, self.peer_addr);

            // Add data to parser
            self.parser.extend(&read_buf[..n]);

            // Process all complete frames with write batching
            loop {
                match self.parser.parse() {
                    Ok(Some(frame)) => {
                        self.handle_frame(frame, n as u64).await?;

                        // Check if we should flush (batch full or buffer large)
                        if self.pending_writes >= WRITE_BATCH_SIZE
                            || self.write_buffer.len() >= WRITE_BUFFER_HIGH_WATER
                        {
                            self.flush_writes().await?;
                        }
                    }
                    Ok(None) => {
                        // No more complete frames - flush any pending writes
                        if self.pending_writes > 0 {
                            self.flush_writes().await?;
                        }
                        break;
                    }
                    Err(e) => {
                        // Protocol error - send error response and continue
                        self.metrics.parse_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let error_frame = Frame::error(format!("ERR {e}"));
                        self.queue_frame(&error_frame);
                        self.flush_writes().await?;
                        self.parser.clear();
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Run in Pub/Sub mode - listen for both commands and subscription messages.
    async fn run_pubsub_mode(&mut self, read_buf: &mut [u8]) -> Result<()> {
        // Take ownership of receivers for async operation
        let mut pubsub_state = self.state.take_pubsub_state();

        // Collect all receivers into a single stream
        let mut channel_receivers: Vec<(Bytes, broadcast::Receiver<PubSubMessage>)> =
            pubsub_state.channel_receivers.drain().collect();
        let mut pattern_receivers: Vec<(Bytes, broadcast::Receiver<PubSubMessage>)> =
            pubsub_state.pattern_receivers.drain().collect();

        loop {
            // Check if connection should close or we left pub/sub mode
            if self.state.is_closed() {
                break;
            }

            // Use tokio::select! to wait for either socket data or pub/sub messages
            tokio::select! {
                // Socket read
                result = self.stream.get_mut().read(read_buf) => {
                    match result {
                        Ok(0) => {
                            debug!("Connection closed by peer in pubsub mode: {}", self.peer_addr);
                            break;
                        }
                        Ok(n) => {
                            trace!("Read {} bytes from {} (pubsub mode)", n, self.peer_addr);
                            self.parser.extend(&read_buf[..n]);

                            // Process commands (limited set in pub/sub mode)
                            while let Ok(Some(frame)) = self.parser.parse() {
                                let should_exit = self.handle_pubsub_command(
                                    frame,
                                    &mut channel_receivers,
                                    &mut pattern_receivers,
                                ).await?;

                                if should_exit {
                                    // Restore state and exit pub/sub mode
                                    pubsub_state.channel_receivers = channel_receivers.into_iter().collect();
                                    pubsub_state.pattern_receivers = pattern_receivers.into_iter().collect();
                                    self.state.restore_pubsub_state(pubsub_state);
                                    self.flush_writes().await?;
                                    return Ok(());
                                }
                            }

                            self.flush_writes().await?;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }

                // Check all channel receivers for messages
                msg = Self::recv_any_message(&mut channel_receivers, &mut pattern_receivers) => {
                    if let Some(msg) = msg {
                        self.send_pubsub_message(&msg).await?;
                    }
                }

                // Timeout to check state periodically
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Just continue to check state
                }
            }
        }

        // Restore state before exiting
        pubsub_state.channel_receivers = channel_receivers.into_iter().collect();
        pubsub_state.pattern_receivers = pattern_receivers.into_iter().collect();
        self.state.restore_pubsub_state(pubsub_state);

        Ok(())
    }

    /// Receive a message from any of the receivers.
    async fn recv_any_message(
        channel_receivers: &mut [(Bytes, broadcast::Receiver<PubSubMessage>)],
        pattern_receivers: &mut [(Bytes, broadcast::Receiver<PubSubMessage>)],
    ) -> Option<PubSubMessage> {
        // Try to receive from any channel
        for (_channel, receiver) in channel_receivers.iter_mut() {
            match receiver.try_recv() {
                Ok(msg) => return Some(msg),
                Err(broadcast::error::TryRecvError::Empty) => continue,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(broadcast::error::TryRecvError::Closed) => continue,
            }
        }

        // Try to receive from any pattern
        for (_pattern, receiver) in pattern_receivers.iter_mut() {
            match receiver.try_recv() {
                Ok(msg) => return Some(msg),
                Err(broadcast::error::TryRecvError::Empty) => continue,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(broadcast::error::TryRecvError::Closed) => continue,
            }
        }

        None
    }

    /// Handle a command in Pub/Sub mode. Only certain commands are allowed.
    /// Returns true if we should exit pub/sub mode.
    async fn handle_pubsub_command(
        &mut self,
        frame: Frame,
        channel_receivers: &mut Vec<(Bytes, broadcast::Receiver<PubSubMessage>)>,
        pattern_receivers: &mut Vec<(Bytes, broadcast::Receiver<PubSubMessage>)>,
    ) -> Result<bool> {
        let cmd = match ParsedCommand::from_frame(frame) {
            Ok(cmd) => cmd,
            Err(e) => {
                let error_frame = Frame::error(e.to_string());
                self.queue_frame(&error_frame);
                return Ok(false);
            }
        };

        let cmd_upper = cmd.name.to_uppercase();

        match cmd_upper.as_str() {
            "SUBSCRIBE" | "SSUBSCRIBE" => {
                // Add new subscriptions
                for channel_bytes in &cmd.args {
                    let channel = channel_bytes.clone();
                    let receiver = self.database.pubsub().subscribe(channel.clone());
                    channel_receivers.push((channel.clone(), receiver));

                    // Send confirmation
                    let response = Frame::Array(vec![
                        Frame::Bulk(Bytes::from_static(b"subscribe")),
                        Frame::Bulk(channel),
                        Frame::Integer((channel_receivers.len() + pattern_receivers.len()) as i64),
                    ]);
                    self.queue_frame(&response);
                }
                Ok(false)
            }

            "PSUBSCRIBE" => {
                // Add new pattern subscriptions
                for pattern_bytes in &cmd.args {
                    let pattern = pattern_bytes.clone();
                    let receiver = self.database.pubsub().psubscribe(pattern.clone());
                    pattern_receivers.push((pattern.clone(), receiver));

                    // Send confirmation
                    let response = Frame::Array(vec![
                        Frame::Bulk(Bytes::from_static(b"psubscribe")),
                        Frame::Bulk(pattern),
                        Frame::Integer((channel_receivers.len() + pattern_receivers.len()) as i64),
                    ]);
                    self.queue_frame(&response);
                }
                Ok(false)
            }

            "UNSUBSCRIBE" | "SUNSUBSCRIBE" => {
                if cmd.args.is_empty() {
                    // Unsubscribe from all channels
                    for (channel, _) in channel_receivers.drain(..) {
                        self.database.pubsub().unsubscribe(&channel);
                        let response = Frame::Array(vec![
                            Frame::Bulk(Bytes::from_static(b"unsubscribe")),
                            Frame::Bulk(channel),
                            Frame::Integer(pattern_receivers.len() as i64),
                        ]);
                        self.queue_frame(&response);
                    }
                } else {
                    for channel_bytes in &cmd.args {
                        if let Some(pos) = channel_receivers.iter().position(|(c, _)| c == channel_bytes) {
                            let (channel, _) = channel_receivers.remove(pos);
                            self.database.pubsub().unsubscribe(&channel);
                            let response = Frame::Array(vec![
                                Frame::Bulk(Bytes::from_static(b"unsubscribe")),
                                Frame::Bulk(channel),
                                Frame::Integer((channel_receivers.len() + pattern_receivers.len()) as i64),
                            ]);
                            self.queue_frame(&response);
                        }
                    }
                }

                // Exit pub/sub mode if no more subscriptions
                Ok(channel_receivers.is_empty() && pattern_receivers.is_empty())
            }

            "PUNSUBSCRIBE" => {
                if cmd.args.is_empty() {
                    // Unsubscribe from all patterns
                    for (pattern, _) in pattern_receivers.drain(..) {
                        self.database.pubsub().punsubscribe(&pattern);
                        let response = Frame::Array(vec![
                            Frame::Bulk(Bytes::from_static(b"punsubscribe")),
                            Frame::Bulk(pattern),
                            Frame::Integer(channel_receivers.len() as i64),
                        ]);
                        self.queue_frame(&response);
                    }
                } else {
                    for pattern_bytes in &cmd.args {
                        if let Some(pos) = pattern_receivers.iter().position(|(p, _)| p == pattern_bytes) {
                            let (pattern, _) = pattern_receivers.remove(pos);
                            self.database.pubsub().punsubscribe(&pattern);
                            let response = Frame::Array(vec![
                                Frame::Bulk(Bytes::from_static(b"punsubscribe")),
                                Frame::Bulk(pattern),
                                Frame::Integer((channel_receivers.len() + pattern_receivers.len()) as i64),
                            ]);
                            self.queue_frame(&response);
                        }
                    }
                }

                // Exit pub/sub mode if no more subscriptions
                Ok(channel_receivers.is_empty() && pattern_receivers.is_empty())
            }

            "PING" => {
                // PING is allowed in pub/sub mode
                let response = if cmd.args.is_empty() {
                    Frame::Array(vec![
                        Frame::Bulk(Bytes::from_static(b"pong")),
                        Frame::Bulk(Bytes::from_static(b"")),
                    ])
                } else {
                    Frame::Array(vec![
                        Frame::Bulk(Bytes::from_static(b"pong")),
                        Frame::Bulk(cmd.args[0].clone()),
                    ])
                };
                self.queue_frame(&response);
                Ok(false)
            }

            "QUIT" => {
                self.state.close();
                Ok(true)
            }

            "RESET" => {
                // RESET exits pub/sub mode
                let response = Frame::simple("RESET");
                self.queue_frame(&response);
                Ok(true)
            }

            _ => {
                // Other commands not allowed in pub/sub mode
                let error = Frame::error(format!(
                    "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    cmd.name
                ));
                self.queue_frame(&error);
                Ok(false)
            }
        }
    }

    /// Send a Pub/Sub message to the client.
    async fn send_pubsub_message(&mut self, msg: &PubSubMessage) -> Result<()> {
        let frame = if let Some(ref pattern) = msg.pattern {
            // Pattern message format: ["pmessage", pattern, channel, message]
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"pmessage")),
                Frame::Bulk(pattern.clone()),
                Frame::Bulk(msg.channel.clone()),
                Frame::Bulk(msg.message.clone()),
            ])
        } else {
            // Channel message format: ["message", channel, message]
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"message")),
                Frame::Bulk(msg.channel.clone()),
                Frame::Bulk(msg.message.clone()),
            ])
        };

        self.queue_frame(&frame);
        self.flush_writes().await?;
        Ok(())
    }

    /// Handle a complete frame (command).
    async fn handle_frame(&mut self, frame: Frame, bytes_in: u64) -> Result<()> {
        trace!("Handling frame: {:?}", frame);
        let start = Instant::now();

        // Parse command
        let cmd = match ParsedCommand::from_frame(frame) {
            Ok(cmd) => cmd,
            Err(e) => {
                self.metrics.parse_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let error_frame = Frame::error(e.to_string());
                self.queue_frame(&error_frame);
                return Ok(());
            }
        };

        trace!("Executing command: {}", cmd.name);

        // Execute command
        let response = match self.executor.execute(cmd, self.state.clone()).await {
            Ok(frame) => frame,
            Err(e) => {
                self.metrics.record_error();
                // Convert error to Redis error response
                Frame::error(e.to_string())
            }
        };

        // Queue response (will be batched)
        let bytes_out = self.queue_frame(&response);

        // Record metrics
        self.metrics.record_command(start.elapsed(), bytes_in, bytes_out as u64);

        Ok(())
    }

    /// Queue a frame for writing (batched).
    /// Returns the number of bytes queued.
    fn queue_frame(&mut self, frame: &Frame) -> usize {
        let start_len = self.write_buffer.len();
        frame.serialize(&mut self.write_buffer);
        self.pending_writes += 1;
        self.write_buffer.len() - start_len
    }

    /// Flush all pending writes to the socket.
    async fn flush_writes(&mut self) -> Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        self.stream.write_all(&self.write_buffer).await?;
        self.stream.flush().await?;
        self.write_buffer.clear();
        self.pending_writes = 0;

        Ok(())
    }

    /// Write a frame immediately (for legacy compatibility).
    #[allow(dead_code)]
    async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        self.queue_frame(frame);
        self.flush_writes().await
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("peer_addr", &self.peer_addr)
            .field("state", &self.state)
            .finish()
    }
}
