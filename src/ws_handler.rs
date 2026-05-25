use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use std::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::errors::ErrorCode;
use crate::models::{ClientMessage, ServerMessage, VersionedServerMessage};
use crate::state::{AppState, Peer};

/// Axum WebSocket handshake handler
pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Connection cap check
    if state.peers.len() >= state.config.max_connections {
        warn!(
            "Rejected WebSocket connection. Connection cap {} reached.",
            state.config.max_connections
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Server connection cap reached",
        )
            .into_response();
    }

    // Origin validation
    let origin = headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if state.config.environment == "production" {
        if !crate::security::is_origin_allowed(origin, &state.config.allowed_origins) {
            warn!("Rejected WebSocket connection from forbidden origin: {}", origin);
            return (StatusCode::FORBIDDEN, "Forbidden Origin").into_response();
        }
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let peer_id = Uuid::new_v4();
    info!("New WebSocket connection established: {}", peer_id);

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<VersionedServerMessage>();

    if let Err(e) = state.add_peer(peer_id, tx.clone()) {
        error!("Failed to add peer {}: {:?}", peer_id, e);
        return;
    }

    let (mut ws_sender, mut ws_receiver) = socket.split();

    let error_tx = tx.clone();

    // WRITE TASK
    let mut write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = msg.to_json();

            match ws_sender.send(Message::Text(json)).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("Write task failed for peer {}: {}", peer_id, e);
                    break;
                }
            }
        }

        info!("Write task ended for peer {}", peer_id);
    });

    // READ TASK
    let state_clone = state.clone();

    let mut read_task = tokio::spawn(async move {
        while let Some(result) = ws_receiver.next().await {
            let msg = match result {
                Ok(m) => m,
                Err(e) => {
                    warn!("WebSocket read error for peer {}: {}", peer_id, e);
                    break;
                }
            };

            match msg {
                Message::Close(_) => {
                    info!("Peer {} closed websocket", peer_id);
                    break;
                }

                Message::Binary(_) => {
                    warn!("Peer {} sent binary websocket data. Disconnecting.", peer_id);

                    let _ = error_tx.send(VersionedServerMessage::new(ServerMessage::Error {
                        code: ErrorCode::MalformedMessage.as_str().to_string(),
                        message: "Binary websocket frames are not allowed".to_string(),
                    }));

                    break;
                }

                Message::Ping(_) => {
                    continue;
                }

                Message::Pong(_) => {
                    continue;
                }

                Message::Text(text) => {
                    if text.len() > state_clone.config.max_message_size {
                        warn!(
                            "Peer {} sent oversized message: {} bytes",
                            peer_id,
                            text.len()
                        );

                        let _ = error_tx.send(VersionedServerMessage::new(ServerMessage::Error {
                            code: ErrorCode::RateLimitExceeded.as_str().to_string(),
                            message: "Message too large".to_string(),
                        }));

                        break;
                    }

                    // Rate limit
                    let rate_ok = {
                        if let Some(mut peer) = state_clone.peers.get_mut(&peer_id) {
                            check_rate_limit(&mut peer)
                        } else {
                            false
                        }
                    };

                    if !rate_ok {
                        warn!("Peer {} exceeded rate limit", peer_id);

                        let _ = error_tx.send(VersionedServerMessage::new(ServerMessage::Error {
                            code: ErrorCode::RateLimitExceeded.as_str().to_string(),
                            message: "Rate limit exceeded".to_string(),
                        }));

                        break;
                    }

                    // Parse JSON
                    let parsed_val: Result<serde_json::Value, _> =
                        serde_json::from_str(&text);

                    let val = match parsed_val {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("Malformed JSON from peer {}: {}", peer_id, e);

                            increment_bad_messages(&state_clone, peer_id);

                            let _ = error_tx.send(VersionedServerMessage::new(
                                ServerMessage::Error {
                                    code: ErrorCode::MalformedMessage.as_str().to_string(),
                                    message: "Invalid JSON".to_string(),
                                },
                            ));

                            if check_bad_messages(&state_clone, peer_id) {
                                break;
                            }

                            continue;
                        }
                    };

                    // Version check
                    let version = val.get("version").and_then(|v| v.as_str());

                    if version != Some("1") {
                        warn!("Invalid protocol version from peer {}", peer_id);

                        increment_bad_messages(&state_clone, peer_id);

                        let _ = error_tx.send(VersionedServerMessage::new(
                            ServerMessage::Error {
                                code: ErrorCode::MalformedMessage.as_str().to_string(),
                                message: "Unsupported protocol version".to_string(),
                            },
                        ));

                        if check_bad_messages(&state_clone, peer_id) {
                            break;
                        }

                        continue;
                    }

                    // Deserialize message
                    match serde_json::from_value::<ClientMessage>(val) {
                        Ok(client_msg) => {
                            if let Err(err) =
                                crate::signaling::handle_message(&state_clone, peer_id, client_msg)
                            {
                                warn!(
                                    "Message handling error for peer {}: {:?}",
                                    peer_id, err
                                );

                                let _ = error_tx.send(VersionedServerMessage::new(
                                    ServerMessage::Error {
                                        code: err.code.as_str().to_string(),
                                        message: err.message,
                                    },
                                ));
                            }
                        }

                        Err(e) => {
                            warn!("Schema error from peer {}: {}", peer_id, e);

                            increment_bad_messages(&state_clone, peer_id);

                            let _ = error_tx.send(VersionedServerMessage::new(
                                ServerMessage::Error {
                                    code: ErrorCode::MalformedMessage.as_str().to_string(),
                                    message: "Invalid schema".to_string(),
                                },
                            ));

                            if check_bad_messages(&state_clone, peer_id) {
                                break;
                            }
                        }
                    }
                }
            }
        }

        info!("Read task ended for peer {}", peer_id);
    });

    // Better lifecycle management
    tokio::select! {
        _ = &mut read_task => {
            warn!("Read task exited for peer {}", peer_id);
            write_task.abort();
        }

        _ = &mut write_task => {
            warn!("Write task exited for peer {}", peer_id);
            read_task.abort();
        }
    }

    // Grace period before cleanup
    info!("Grace cleanup wait for peer {}", peer_id);
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // If peer already removed, skip
    if !state.peers.contains_key(&peer_id) {
        info!("Peer {} already cleaned up", peer_id);
        return;
    }

    info!("Final cleanup for peer {}", peer_id);

    let notifications = state.remove_peer(peer_id);

    for (target_id, msg) in notifications {
        if let Some(peer) = state.peers.get(&target_id) {
            let _ = peer.tx.send(msg);
        }
    }
}

/// Rate limiter
fn check_rate_limit(peer: &mut Peer) -> bool {
    let now = Instant::now();
    let elapsed = now.duration_since(peer.rate_limit_last_check).as_secs();

    if elapsed > 0 {
        peer.rate_limit_tokens =
            std::cmp::min(30, peer.rate_limit_tokens + elapsed as usize);
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
        peer.bad_message_count >= 3
    } else {
        true
    }
}