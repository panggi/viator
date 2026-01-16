//! Pub/Sub command implementations.
//!
//! Commands for subscribing to channels and patterns.

use super::ParsedCommand;
use crate::Result;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// SUBSCRIBE channel [channel ...]
pub fn cmd_subscribe(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let pubsub = db.pubsub();
        let mut responses = Vec::new();

        for channel_bytes in &cmd.args {
            let channel = channel_bytes.clone();
            let receiver = pubsub.subscribe(channel.clone());
            client.subscribe_channel(channel.clone(), receiver);

            // Response format: ["subscribe", channel, subscription_count]
            responses.push(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"subscribe")),
                Frame::Bulk(channel),
                Frame::Integer(client.total_subscription_count() as i64),
            ]));
        }

        // Return the last response (Redis behavior)
        Ok(responses.pop().unwrap_or(Frame::Null))
    })
}

/// PSUBSCRIBE pattern [pattern ...]
pub fn cmd_psubscribe(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let pubsub = db.pubsub();
        let mut responses = Vec::new();

        for pattern_bytes in &cmd.args {
            let pattern = pattern_bytes.clone();
            let receiver = pubsub.psubscribe(pattern.clone());
            client.subscribe_pattern(pattern.clone(), receiver);

            // Response format: ["psubscribe", pattern, subscription_count]
            responses.push(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"psubscribe")),
                Frame::Bulk(pattern),
                Frame::Integer(client.total_subscription_count() as i64),
            ]));
        }

        Ok(responses.pop().unwrap_or(Frame::Null))
    })
}

/// UNSUBSCRIBE [channel ...]
pub fn cmd_unsubscribe(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let pubsub = db.pubsub();
        let mut responses = Vec::new();

        if cmd.args.is_empty() {
            // Unsubscribe from all channels
            let channels = client.unsubscribe_all_channels();
            for channel in channels {
                pubsub.unsubscribe(&channel);
                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"unsubscribe")),
                    Frame::Bulk(channel),
                    Frame::Integer(client.total_subscription_count() as i64),
                ]));
            }
            if responses.is_empty() {
                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"unsubscribe")),
                    Frame::Null,
                    Frame::Integer(0),
                ]));
            }
        } else {
            for channel_bytes in &cmd.args {
                let channel = channel_bytes.clone();
                client.unsubscribe_channel(&channel);
                pubsub.unsubscribe(&channel);

                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"unsubscribe")),
                    Frame::Bulk(channel),
                    Frame::Integer(client.total_subscription_count() as i64),
                ]));
            }
        }

        Ok(responses.pop().unwrap_or(Frame::Null))
    })
}

/// PUNSUBSCRIBE [pattern ...]
pub fn cmd_punsubscribe(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let pubsub = db.pubsub();
        let mut responses = Vec::new();

        if cmd.args.is_empty() {
            // Unsubscribe from all patterns
            let patterns = client.unsubscribe_all_patterns();
            for pattern in patterns {
                pubsub.punsubscribe(&pattern);
                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"punsubscribe")),
                    Frame::Bulk(pattern),
                    Frame::Integer(client.total_subscription_count() as i64),
                ]));
            }
            if responses.is_empty() {
                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"punsubscribe")),
                    Frame::Null,
                    Frame::Integer(0),
                ]));
            }
        } else {
            for pattern_bytes in &cmd.args {
                let pattern = pattern_bytes.clone();
                client.unsubscribe_pattern(&pattern);
                pubsub.punsubscribe(&pattern);

                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"punsubscribe")),
                    Frame::Bulk(pattern),
                    Frame::Integer(client.total_subscription_count() as i64),
                ]));
            }
        }

        Ok(responses.pop().unwrap_or(Frame::Null))
    })
}

/// PUBLISH channel message (already in server_cmds, but adding here for completeness)
pub fn cmd_publish(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;

        let channel = cmd.args[0].clone();
        let message = cmd.args[1].clone();

        let count = db.pubsub().publish(channel, message);
        Ok(Frame::Integer(count as i64))
    })
}

/// PUBSUB subcommand [argument ...]
pub fn cmd_pubsub(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let subcommand = cmd.get_str(0)?.to_uppercase();
        let pubsub = db.pubsub();

        match subcommand.as_str() {
            "CHANNELS" => {
                let pattern = cmd.args.get(1).cloned();
                let channels = pubsub.channels(pattern.as_ref());
                Ok(Frame::Array(
                    channels.into_iter().map(Frame::Bulk).collect(),
                ))
            }
            "NUMSUB" => {
                let mut result = Vec::new();
                for channel in cmd.args.iter().skip(1) {
                    result.push(Frame::Bulk(channel.clone()));
                    result.push(Frame::Integer(pubsub.numsub(channel) as i64));
                }
                Ok(Frame::Array(result))
            }
            "NUMPAT" => Ok(Frame::Integer(pubsub.numpat() as i64)),
            "SHARDCHANNELS" => {
                // For single-node, sharded channels work like regular channels
                let pattern = cmd.args.get(1).cloned();
                let channels = pubsub.channels(pattern.as_ref());
                Ok(Frame::Array(
                    channels.into_iter().map(Frame::Bulk).collect(),
                ))
            }
            "SHARDNUMSUB" => {
                // For single-node, sharded numsub works like regular numsub
                let mut result = Vec::new();
                for channel in cmd.args.iter().skip(1) {
                    result.push(Frame::Bulk(channel.clone()));
                    result.push(Frame::Integer(pubsub.numsub(channel) as i64));
                }
                Ok(Frame::Array(result))
            }
            _ => Err(crate::error::CommandError::SyntaxError.into()),
        }
    })
}

/// SPUBLISH shardchannel message
/// Publish a message to a shard channel (cluster-aware pub/sub)
/// In single-node mode, behaves like regular PUBLISH
pub fn cmd_spublish(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_exact_args(2)?;

        let channel = cmd.args[0].clone();
        let message = cmd.args[1].clone();

        // In single-node mode, sharded publish works like regular publish
        let count = db.pubsub().publish(channel, message);
        Ok(Frame::Integer(count as i64))
    })
}

/// SSUBSCRIBE shardchannel [shardchannel ...]
/// Subscribe to shard channels (cluster-aware pub/sub)
/// In single-node mode, behaves like regular SUBSCRIBE
pub fn cmd_ssubscribe(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(1)?;

        let pubsub = db.pubsub();
        let mut responses = Vec::new();

        for channel_bytes in &cmd.args {
            let channel = channel_bytes.clone();
            let receiver = pubsub.subscribe(channel.clone());
            client.subscribe_channel(channel.clone(), receiver);

            // Response format: ["ssubscribe", shardchannel, subscription_count]
            responses.push(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"ssubscribe")),
                Frame::Bulk(channel),
                Frame::Integer(client.total_subscription_count() as i64),
            ]));
        }

        Ok(responses.pop().unwrap_or(Frame::Null))
    })
}

/// SUNSUBSCRIBE [shardchannel ...]
/// Unsubscribe from shard channels (cluster-aware pub/sub)
/// In single-node mode, behaves like regular UNSUBSCRIBE
pub fn cmd_sunsubscribe(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        let pubsub = db.pubsub();
        let mut responses = Vec::new();

        if cmd.args.is_empty() {
            // Unsubscribe from all channels
            let channels = client.unsubscribe_all_channels();
            for channel in channels {
                pubsub.unsubscribe(&channel);
                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"sunsubscribe")),
                    Frame::Bulk(channel),
                    Frame::Integer(client.total_subscription_count() as i64),
                ]));
            }
            if responses.is_empty() {
                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"sunsubscribe")),
                    Frame::Null,
                    Frame::Integer(0),
                ]));
            }
        } else {
            for channel_bytes in &cmd.args {
                let channel = channel_bytes.clone();
                client.unsubscribe_channel(&channel);
                pubsub.unsubscribe(&channel);

                responses.push(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"sunsubscribe")),
                    Frame::Bulk(channel),
                    Frame::Integer(client.total_subscription_count() as i64),
                ]));
            }
        }

        Ok(responses.pop().unwrap_or(Frame::Null))
    })
}
