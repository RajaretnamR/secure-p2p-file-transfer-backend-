use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, State},
    response::IntoResponse,
    http::{HeaderMap, StatusCode},
};
use futures::{StreamExt, SinkExt};
use uuid::Uuid;
use std::time::Instant;
use tracing::{info, warn, error};

use crate::state::{AppState, Peer};
use crate::models::{ClientMessage, ServerMessage, VersionedServerMessage};
use crate::errors::ErrorCode;

/// Axum WebSocket handshake handler. Validates origin and connection caps.
pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // 1. Connection cap check
    if state.peers.len() >= state.config.max_connections {
        warn!("Rejected WebSocket connection. Connection cap of {} reached.", state.config.max_connections);
        return (StatusCode::SERVICE_UNAVAILABLE, "Server connection cap reached").into_response();
    }

    // 2. CORS / Origin validation
    let origin = headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if state.config.environment == "production" {
        if !crate::security::is_origin_allowed(origin, &state.config.allowed_origins) {
            warn!("Rejected WebSocket connection from unauthorized Origin: '{}'", origin);
            return (StatusCode::FORBIDDEN, "Forbidden Origin").into_response();
        }
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Manages WebSocket read/write loops, rate limiting, and message sanitization
async fn handle_socket(socket: WebSocket, state: AppState) {
    let peer_id = Uuid::new_v4();
    info!("New WebSocket connection established. Temporary peer ID: {}", peer_id);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<VersionedServerMessage>();
    
    // Register peer in state
    if let Err(e) = state.add_peer(peer_id, tx.clone()) {
        error!("Failed to add peer {} to state: {:?}", peer_id, e);
        return;
    }

    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Write task: sends out messages queued in peer's mpsc receiver channel
    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json_str = msg.to_json();
            if ws_sender.send(Message::Text(json_str)).await.is_err() {
                break;
            }
        }
        info!("Write task shut down for peer {}", peer_id);
    });

    let error_tx = tx.clone();

    // Read loop: processes incoming frames from client
    let state_clone = state.clone();
    let read_task = tokio::spawn(async move {
        let state = state_clone;
        while let Some(result) = ws_receiver.next().await {
            let msg = match result {
                Ok(m) => m,
                Err(e) => {
                    warn!("WebSocket read error for peer {}: {}", peer_id, e);
                    break;
                }
            };

            // 1. Binary message rejection (immediate disconnect)
            if let Message::Binary(_) = msg {
                warn!("Peer {} sent binary data. Terminating connection immediately.", peer_id);
                break;
            }

            if let Message::Close(_) = msg {
                break;
            }

            if let Message::Text(text) = msg {
                // 2. Max size validation (64KB cap)
                if text.len() > state.config.max_message_size {
                    warn!("Peer {} sent oversized message ({} bytes). Disconnecting.", peer_id, text.len());
                    let _ = error_tx.send(VersionedServerMessage::new(ServerMessage::Error {
                        code: ErrorCode::RateLimitExceeded.as_str().to_string(),
                        message: "Message size limit exceeded".to_string(),
                    }));
                    break;
                }

                // 3. Per-connection rate limiting
                let rate_ok = {
                    if let Some(mut peer) = state.peers.get_mut(&peer_id) {
                        check_rate_limit(&mut peer)
                    } else {
                        false
                    }
                };

                if !rate_ok {
                    warn!("Peer {} exceeded rate limit. Disconnecting.", peer_id);
                    let _ = error_tx.send(VersionedServerMessage::new(ServerMessage::Error {
                        code: ErrorCode::RateLimitExceeded.as_str().to_string(),
                        message: "Rate limit exceeded. Disconnecting.".to_string(),
                    }));
                    break;
                }

                // 4. Parse JSON
                let parsed_val: Result<serde_json::Value, _> = serde_json::from_str(&text);
                let val = match parsed_val {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("Peer {} sent malformed JSON: {}", peer_id, e);
                        increment_bad_messages(&state, peer_id);
                        let _ = error_tx.send(VersionedServerMessage::new(ServerMessage::Error {
                            code: ErrorCode::MalformedMessage.as_str().to_string(),
                            message: format!("Invalid JSON: {}", e),
                        }));
                        if check_bad_messages(&state, peer_id) {
                            break;
                        }
                        continue;
                    }
                };

                // 5. Check protocol version
                let version = val.get("version").and_then(|v| v.as_str());
                if version != Some("1") {
                    warn!("Peer {} sent message with invalid or missing version: {:?}", peer_id, version);
                    increment_bad_messages(&state, peer_id);
                    let _ = error_tx.send(VersionedServerMessage::new(ServerMessage::Error {
                        code: ErrorCode::MalformedMessage.as_str().to_string(),
                        message: "Missing or unsupported protocol version (expected '1')".to_string(),
                    }));
                    if check_bad_messages(&state, peer_id) {
                        break;
                    }
                    continue;
                }

                // 6. Deserialize and handle message
                match serde_json::from_value::<ClientMessage>(val) {
                    Ok(client_msg) => {
                        if let Err(err) = crate::signaling::handle_message(&state, peer_id, client_msg) {
                            warn!("Error handling message from peer {}: {:?}", peer_id, err);
                            
                            if matches!(err.code, ErrorCode::Unauthorized | ErrorCode::InvalidSession | ErrorCode::InvalidRole) {
                                increment_bad_messages(&state, peer_id);
                            }

                            let _ = error_tx.send(VersionedServerMessage::new(ServerMessage::Error {
                                code: err.code.as_str().to_string(),
                                message: err.message,
                            }));

                            if check_bad_messages(&state, peer_id) {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Peer {} sent JSON with invalid schema: {}", peer_id, e);
                        increment_bad_messages(&state, peer_id);
                        let _ = error_tx.send(VersionedServerMessage::new(ServerMessage::Error {
                            code: ErrorCode::MalformedMessage.as_str().to_string(),
                            message: format!("Invalid message schema: {}", e),
                        }));
                        if check_bad_messages(&state, peer_id) {
                            break;
                        }
                    }
                }
            }
        }
        info!("Read task shut down for peer {}", peer_id);
    });

    // Wait for either read task or write task to terminate
    tokio::select! {
        _ = write_task => {},
        _ = read_task => {},
    }

    // Clean up peer state and notify session partner
    info!("Tearing down connection for peer {}", peer_id);
    let notifications = state.remove_peer(peer_id);
    for (target_id, msg) in notifications {
        if let Some(peer) = state.peers.get(&target_id) {
            let _ = peer.tx.send(msg);
        }
    }
}

/// Token bucket check: replenishes 1 token per second, max 30 tokens
fn check_rate_limit(peer: &mut Peer) -> bool {
    let now = Instant::now();
    let elapsed = now.duration_since(peer.rate_limit_last_check).as_secs();
    if elapsed > 0 {
        peer.rate_limit_tokens = std::cmp::min(30, peer.rate_limit_tokens + elapsed as usize);
        peer.rate_limit_last_check = now;
    }
    if peer.rate_limit_tokens == 0 {
        false
    } else {
        peer.rate_limit_tokens -= 1;
        true
    }
}

fn increment_bad_messages(state: &AppState, peer_id: Uuid) {
    if let Some(mut peer) = state.peers.get_mut(&peer_id) {
        peer.bad_message_count += 1;
    }
}

fn check_bad_messages(state: &AppState, peer_id: Uuid) -> bool {
    if let Some(peer) = state.peers.get(&peer_id) {
        if peer.bad_message_count >= 3 {
            warn!("Peer {} disconnected: reached bad message threshold", peer_id);
            true
        } else {
            false
        }
    } else {
        true
    }
}
