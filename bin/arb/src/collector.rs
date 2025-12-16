use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use burberry::{async_trait, Collector, CollectorStream};
use eyre::Result;
use fastcrypto::encoding::{Base64, Encoding};
use futures::stream::StreamExt;
use interprocess::local_socket::{
    tokio::{prelude::*, Stream},
    GenericNamespaced,
};
use serde::Deserialize;
use sui_json_rpc_types::{BcsEvent, SuiEvent, SuiTransactionBlockEffects};
use sui_types::{digests::TransactionDigest, effects::TransactionEffects, event::EventID, transaction::TransactionData};
use tokio::{io::AsyncReadExt, pin, time};
use tracing::{debug, error, info, warn};

use crate::types::Event;

// Generated proto types
pub mod sui_rpc_v2 {
    tonic::include_proto!("sui.rpc.v2");
}

use sui_rpc_v2::subscription_service_client::SubscriptionServiceClient;
use sui_rpc_v2::SubscribeCheckpointsRequest;
use tonic::transport::Endpoint;

pub struct PublicTxCollector {
    path: String,
}

impl PublicTxCollector {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string() }
    }

    async fn connect(&self) -> Result<Stream> {
        let name = self.path.as_str().to_ns_name::<GenericNamespaced>()?;
        let conn = Stream::connect(name).await?;
        Ok(conn)
    }
}

#[async_trait]
impl Collector<Event> for PublicTxCollector {
    fn name(&self) -> &str {
        "PublicTxCollector"
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, Event>> {
        let mut conn = self.connect().await?;
        let mut effects_len_buf = [0u8; 4];
        let mut events_len_buf = [0u8; 4];

        let stream = async_stream::stream! {
            loop {
                tokio::select! {
                    result = conn.read_exact(&mut effects_len_buf) => {
                        if result.is_err() {
                            debug!("Failed to read effects length");
                            conn = self.connect().await.expect("Failed to reconnect to tx socket");
                            continue;
                        }

                        let effects_len = u32::from_be_bytes(effects_len_buf);
                        let mut effects_buf = vec![0u8; effects_len as usize];
                        if conn.read_exact(&mut effects_buf).await.is_err() {
                            debug!("Failed to read effects");
                            conn = self.connect().await.expect("Failed to reconnect to tx socket");
                            continue;
                        }

                        if conn.read_exact(&mut events_len_buf).await.is_err() {
                            debug!("Failed to read events length");
                            conn = self.connect().await.expect("Failed to reconnect to tx socket");
                            continue;
                        }

                        let events_len = u32::from_be_bytes(events_len_buf);
                        let mut events_buf = vec![0u8; events_len as usize];
                        if conn.read_exact(&mut events_buf).await.is_err() {
                            debug!("Failed to read events");
                            conn = self.connect().await.expect("Failed to reconnect to tx socket");
                            continue;
                        }

                        let tx_effects: TransactionEffects = match bincode::deserialize(&effects_buf) {
                            Ok(tx_effects) => tx_effects,
                            Err(e) => {
                                error!("Invalid tx_effects: {:?}", e);
                                continue;
                            }
                        };

                        let events: Vec<SuiEvent> = if events_len == 0 {
                            vec![]
                        } else {
                            match serde_json::from_slice(&events_buf) {
                                Ok(events) => events,
                                Err(e) => {
                                    error!("Invalid events: {:?}", e);
                                    continue;
                                }
                            }
                        };

                        if let Ok(tx_effects) = SuiTransactionBlockEffects::try_from(tx_effects) {
                            yield Event::PublicTx(tx_effects, events);
                        }

                    }
                    else => {
                        time::sleep(time::Duration::from_millis(10)).await;
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TxMessage {
    tx_bytes: String,
}

impl TryFrom<TxMessage> for TransactionData {
    type Error = eyre::Error;

    fn try_from(tx_message: TxMessage) -> Result<Self> {
        let tx_bytes = Base64::decode(&tx_message.tx_bytes)?;
        let tx_data: TransactionData = bcs::from_bytes(&tx_bytes)?;
        Ok(tx_data)
    }
}

pub struct PrivateTxCollector {
    ws_url: String,
}

impl PrivateTxCollector {
    pub fn new(ws_url: &str) -> Self {
        Self {
            ws_url: ws_url.to_string(),
        }
    }
}

/// Collector that subscribes to Sui's gRPC checkpoint stream.
///
/// Uses the SubscriptionService.SubscribeCheckpoints RPC to receive
/// checkpoints as they are finalized. Checkpoints are guaranteed to
/// arrive in-order and without gaps.
pub struct GrpcCheckpointCollector {
    grpc_endpoint: String,
    last_checkpoint: Arc<AtomicU64>,
    max_disconnect_duration: Duration,
}

impl GrpcCheckpointCollector {
    pub fn new(grpc_endpoint: &str, max_disconnect_mins: u64) -> Self {
        Self {
            grpc_endpoint: grpc_endpoint.to_string(),
            last_checkpoint: Arc::new(AtomicU64::new(0)),
            max_disconnect_duration: Duration::from_secs(max_disconnect_mins * 60),
        }
    }

}

/// Convert prost_types::Value to serde_json::Value
fn prost_value_to_serde_json(value: prost_types::Value) -> Option<serde_json::Value> {
    use prost_types::value::Kind;

    match value.kind? {
        Kind::NullValue(_) => Some(serde_json::Value::Null),
        Kind::NumberValue(n) => Some(serde_json::Value::Number(
            serde_json::Number::from_f64(n)?,
        )),
        Kind::StringValue(s) => Some(serde_json::Value::String(s)),
        Kind::BoolValue(b) => Some(serde_json::Value::Bool(b)),
        Kind::StructValue(s) => {
            let map: serde_json::Map<String, serde_json::Value> = s
                .fields
                .into_iter()
                .filter_map(|(k, v)| Some((k, prost_value_to_serde_json(v)?)))
                .collect();
            Some(serde_json::Value::Object(map))
        }
        Kind::ListValue(l) => {
            let arr: Vec<serde_json::Value> = l
                .values
                .into_iter()
                .filter_map(prost_value_to_serde_json)
                .collect();
            Some(serde_json::Value::Array(arr))
        }
    }
}

#[async_trait]
impl Collector<Event> for GrpcCheckpointCollector {
    fn name(&self) -> &str {
        "GrpcCheckpointCollector"
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, Event>> {
        let grpc_endpoint = self.grpc_endpoint.clone();
        let last_checkpoint = self.last_checkpoint.clone();
        let max_disconnect_duration = self.max_disconnect_duration;

        let stream = async_stream::stream! {
            info!("GrpcCheckpointCollector stream started, endpoint: {}", grpc_endpoint);
            let mut disconnect_start: Option<Instant> = None;

            loop {
                info!("Connecting to gRPC endpoint: {}", grpc_endpoint);

                // Configure endpoint with HTTP/2 keep-alive to prevent idle disconnects
                let endpoint = match Endpoint::from_shared(grpc_endpoint.clone()) {
                    Ok(ep) => ep
                        .http2_keep_alive_interval(Duration::from_secs(30))
                        .keep_alive_timeout(Duration::from_secs(10))
                        .keep_alive_while_idle(true),
                    Err(e) => {
                        error!(?e, "Invalid gRPC endpoint URL");
                        continue;
                    }
                };

                let connect_result = SubscriptionServiceClient::connect(endpoint).await;

                match connect_result {
                    Ok(mut client) => {
                        info!(
                            endpoint = %grpc_endpoint,
                            last_checkpoint = last_checkpoint.load(Ordering::SeqCst),
                            "Connected to gRPC checkpoint stream"
                        );
                        disconnect_start = None;

                        // Field paths are relative to Checkpoint (not prefixed with "checkpoint.")
                        let request = SubscribeCheckpointsRequest {
                            read_mask: Some(prost_types::FieldMask {
                                paths: vec![
                                    "sequence_number".into(),
                                    "digest".into(),
                                    "transactions".into(),
                                ],
                            }),
                        };

                        match client.subscribe_checkpoints(request).await {
                            Ok(response) => {
                                debug!("Subscription established, waiting for checkpoints...");
                                let mut checkpoint_stream = response.into_inner();

                                while let Some(resp) = checkpoint_stream.next().await {
                                    match resp {
                                        Ok(checkpoint_resp) => {
                                            let cursor = checkpoint_resp.cursor.unwrap_or(0);
                                            let tx_count = checkpoint_resp
                                                .checkpoint
                                                .as_ref()
                                                .map(|c| c.transactions.len())
                                                .unwrap_or(0);

                                            debug!(
                                                checkpoint = cursor,
                                                transactions = tx_count,
                                                "Received checkpoint from gRPC"
                                            );

                                            // Process checkpoint and yield events
                                            let events = process_checkpoint_internal(
                                                &last_checkpoint,
                                                checkpoint_resp,
                                            );

                                            let event_count = events.len();
                                            for event in events {
                                                yield event;
                                            }

                                            debug!(
                                                checkpoint = cursor,
                                                events_yielded = event_count,
                                                "Yielded events to engine"
                                            );
                                        }
                                        Err(e) => {
                                            warn!(?e, "gRPC stream error, will reconnect");
                                            break;
                                        }
                                    }
                                }

                                warn!("gRPC checkpoint stream ended");
                            }
                            Err(e) => {
                                error!(?e, "Failed to subscribe to checkpoints");
                            }
                        }
                    }
                    Err(e) => {
                        error!(?e, endpoint = %grpc_endpoint, "Failed to connect to gRPC endpoint");
                    }
                }

                // Track disconnect duration and exit if too long
                let start = disconnect_start.get_or_insert_with(Instant::now);
                if start.elapsed() > max_disconnect_duration {
                    error!(
                        duration = ?start.elapsed(),
                        max_duration = ?max_disconnect_duration,
                        "Failed to reconnect within timeout, shutting down"
                    );
                    std::process::exit(1);
                }

                // Exponential backoff would be better, but simple 1s sleep for now
                let backoff = Duration::from_secs(1);
                warn!(
                    last_checkpoint = last_checkpoint.load(Ordering::SeqCst),
                    backoff_secs = backoff.as_secs(),
                    "Reconnecting to gRPC stream..."
                );
                tokio::time::sleep(backoff).await;
            }
        };

        Ok(Box::pin(stream))
    }
}

/// Helper function to process checkpoint (used inside async_stream)
fn process_checkpoint_internal(
    last_checkpoint: &AtomicU64,
    resp: sui_rpc_v2::SubscribeCheckpointsResponse,
) -> Vec<Event> {
    let cursor = resp.cursor.unwrap_or(0);
    last_checkpoint.store(cursor, Ordering::SeqCst);

    let Some(checkpoint) = resp.checkpoint else {
        debug!(checkpoint = cursor, "No checkpoint data in response");
        return vec![];
    };

    let total_txs = checkpoint.transactions.len();
    let mut effects_parsed = 0usize;
    let mut effects_failed = 0usize;

    let mut events = Vec::new();

    for tx in checkpoint.transactions {
        // Get transaction digest
        let tx_digest = tx
            .digest
            .as_ref()
            .and_then(|d| d.parse::<TransactionDigest>().ok())
            .unwrap_or_default();

        // Extract effects from BCS
        let effects = tx
            .effects
            .and_then(|e| e.bcs)
            .and_then(|bcs| bcs.value)
            .and_then(|bytes| bcs::from_bytes::<TransactionEffects>(&bytes).ok())
            .and_then(|e| SuiTransactionBlockEffects::try_from(e).ok());

        let Some(effects) = effects else {
            effects_failed += 1;
            continue;
        };

        effects_parsed += 1;

        // Convert events from proto fields
        let sui_events = convert_proto_events(tx.events, tx_digest);

        events.push(Event::PublicTx(effects, sui_events));
    }

    debug!(
        checkpoint = cursor,
        total_txs,
        effects_parsed,
        effects_failed,
        events_created = events.len(),
        "Processed checkpoint"
    );

    events
}

/// Convert proto events to SuiEvent format
fn convert_proto_events(
    proto_events: Option<sui_rpc_v2::TransactionEvents>,
    tx_digest: TransactionDigest,
) -> Vec<SuiEvent> {
    let Some(tx_events) = proto_events else {
        return vec![];
    };

    // Convert from proto Event fields - the proto already provides parsed JSON
    tx_events
        .events
        .into_iter()
        .enumerate()
        .filter_map(|(event_seq, proto_event)| {
            let event_type = proto_event.event_type?;
            let package_id = proto_event.package_id.and_then(|s| s.parse().ok())?;
            let sender = proto_event.sender.and_then(|s| s.parse().ok())?;
            let module = proto_event.module.and_then(|s| s.parse().ok())?;

            // Parse JSON content from proto
            let has_json = proto_event.json.is_some();
            let parsed_json = proto_event
                .json
                .and_then(prost_value_to_serde_json)
                .unwrap_or(serde_json::Value::Null);

            // debug!(
            //     event_type = %event_type,
            //     has_json = has_json,
            //     parsed_json = %parsed_json,
            //     "Proto event conversion"
            // );

            // BCS content
            let bcs_bytes = proto_event
                .contents
                .and_then(|b| b.value)
                .unwrap_or_default();

            Some(SuiEvent {
                id: EventID {
                    tx_digest,
                    event_seq: event_seq as u64,
                },
                package_id,
                transaction_module: module,
                sender,
                type_: event_type.parse().ok()?,
                parsed_json,
                bcs: BcsEvent::Base64 { bcs: bcs_bytes },
                timestamp_ms: None,
            })
        })
        .collect()
}

#[async_trait]
impl Collector<Event> for PrivateTxCollector {
    fn name(&self) -> &str {
        "PrivateTxCollector"
    }

    async fn get_event_stream(&self) -> Result<CollectorStream<'_, Event>> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&self.ws_url)
            .await
            .expect("Failed to connect to relay server");

        let (_, read) = ws_stream.split();

        let stream = async_stream::stream! {
            pin!(read);
            while let Some(message) = read.next().await {
                let message = match message {
                    Ok(msg) => msg,
                    Err(e) => {
                        error!("Relay websocket error: {:?}", e);
                        continue;
                    }
                };

                let tx_message: TxMessage = serde_json::from_str(message.to_text().unwrap()).unwrap();
                let tx_data = match TransactionData::try_from(tx_message) {
                    Ok(tx_data) => tx_data,
                    Err(e) => {
                        error!("Invalid tx_message: {:?}", e);
                        continue;
                    }
                };

                yield Event::PrivateTx(tx_data);
            }
        };

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test gRPC checkpoint subscription
    /// Run with: cargo test -p arb grpc_checkpoint_test -- --nocapture
    #[tokio::test]
    async fn grpc_checkpoint_test() {
        // Use mainnet public endpoint - MUST use https:// for TLS
        // NOT grpc:// - that's not a valid scheme for tonic
        let endpoint = std::env::var("SUI_GRPC_URL")
            .unwrap_or_else(|_| "https://fullnode.mainnet.sui.io:443".to_string());

        println!("\n=== gRPC Checkpoint Subscription Test ===\n");
        println!("Endpoint: {}", endpoint);
        println!("NOTE: Use https:// scheme, not grpc://\n");

        println!("Step 1: Connecting...");
        let connect_result = SubscriptionServiceClient::connect(endpoint.clone()).await;

        match connect_result {
            Ok(mut client) => {
                println!("✓ Connected successfully!\n");

                // Field paths are relative to Checkpoint, not SubscribeCheckpointsResponse
                let request = SubscribeCheckpointsRequest {
                    read_mask: Some(prost_types::FieldMask {
                        paths: vec![
                            "sequence_number".into(),
                            "digest".into(),
                            "transactions".into(),
                        ],
                    }),
                };

                println!("Step 2: Subscribing to checkpoints (correct field mask)...");

                match client.subscribe_checkpoints(request).await {
                    Ok(response) => {
                        println!("✓ Subscription established!\n");
                        let mut stream = response.into_inner();

                        println!("Step 3: Receiving checkpoints (will receive 3 then stop)...\n");

                        // Try to receive up to 3 checkpoints
                        for i in 0..3 {
                            print!("  Waiting for checkpoint {}... ", i + 1);

                            match tokio::time::timeout(
                                std::time::Duration::from_secs(60),
                                stream.message(),
                            )
                            .await
                            {
                                Ok(Ok(Some(checkpoint_resp))) => {
                                    let cursor = checkpoint_resp.cursor.unwrap_or(0);

                                    // Debug: show what fields are present
                                    println!("✓ checkpoint={}", cursor);
                                    println!("    Raw response:");
                                    println!("      cursor: {:?}", checkpoint_resp.cursor);
                                    println!("      checkpoint.is_some: {}", checkpoint_resp.checkpoint.is_some());
                                    if let Some(ref cp) = checkpoint_resp.checkpoint {
                                        println!("      checkpoint.sequence_number: {:?}", cp.sequence_number);
                                        println!("      checkpoint.digest: {:?}", cp.digest);
                                        println!("      checkpoint.transactions.len: {}", cp.transactions.len());
                                        println!("      checkpoint.contents.is_some: {}", cp.contents.is_some());
                                        println!("      checkpoint.summary.is_some: {}", cp.summary.is_some());
                                    }

                                    let tx_count = checkpoint_resp
                                        .checkpoint
                                        .as_ref()
                                        .map(|c| c.transactions.len())
                                        .unwrap_or(0);

                                    // Show sample transactions
                                    if let Some(checkpoint) = &checkpoint_resp.checkpoint {
                                        for (j, tx) in checkpoint.transactions.iter().take(2).enumerate() {
                                            let digest = tx.digest.as_deref().unwrap_or("?");
                                            let event_count = tx
                                                .events
                                                .as_ref()
                                                .map(|e| e.events.len())
                                                .unwrap_or(0);
                                            let has_effects = tx.effects.is_some();
                                            println!(
                                                "    tx[{}]: {} (events={}, effects={})",
                                                j,
                                                &digest[..20.min(digest.len())],
                                                event_count,
                                                has_effects
                                            );
                                        }
                                    }

                                    // Test our conversion
                                    let events = process_checkpoint_internal(
                                        &AtomicU64::new(0),
                                        checkpoint_resp,
                                    );
                                    println!("    → Converted to {} Event::PublicTx", events.len());
                                }
                                Ok(Ok(None)) => {
                                    println!("✗ Stream ended unexpectedly");
                                    panic!("Stream ended");
                                }
                                Ok(Err(e)) => {
                                    println!("✗ Error: {:?}", e);
                                    panic!("Stream error: {:?}", e);
                                }
                                Err(_) => {
                                    println!("✗ Timeout (60s)");
                                    panic!("Timeout waiting for checkpoint");
                                }
                            }
                        }

                        println!("\n✓ Test passed! gRPC checkpoint streaming works.\n");
                    }
                    Err(e) => {
                        println!("✗ Failed to subscribe: {:?}", e);
                        panic!("Subscribe failed: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("✗ Failed to connect: {:?}", e);
                println!("\nTroubleshooting:");
                println!("  - Make sure to use https:// not grpc://");
                println!("  - Try: SUI_GRPC_URL=https://fullnode.mainnet.sui.io:443");
                panic!("Connection failed: {:?}", e);
            }
        }
    }
}
