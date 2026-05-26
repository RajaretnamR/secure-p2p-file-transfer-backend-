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

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
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

    let origin = headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if state.config.environment == "production" {
        if !crate::security::is_origin_allowed(
            origin,
            &state.config.allowed_origins,
        ) {
            warn!(
                "Rejected forbidden origin websocket connection: {}",
                origin
            );

            return (
                StatusCode::FORBIDDEN,
                "Forbidden Origin",
            )
                .into_response();
        }
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let peer_id = Uuid::new_v4();

    info!("WebSocket connected: {}", peer_id);

    let (tx, mut rx) =
        tokio::sync::mpsc::unbounded_channel::<VersionedServerMessage>();

    if let Err(e) = state.add_peer(peer_id, tx.clone()) {
        error!(
            "Failed to register peer {}: {:?}",
            peer_id,
            e
        );
        return;
    }

    let (mut ws_sender, mut ws_receiver) = socket.split();

    let state_for_read = state.clone();
    let tx_for_read = tx.clone();

    let write_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = msg.to_json();

            match ws_sender.send(Message::Text(json)).await {
                Ok(_) => {}
                Err(err) => {
                    warn!(
                        "Write failed for peer {}: {}",
                        peer_id,
                        err
                    );
                    break;
                }
            }
        }

        info!("Write task ended for peer {}", peer_id);
    });

    let read_task = tokio::spawn(async move {
        while let Some(result) = ws_receiver.next().await {
            let msg = match result {
                Ok(msg) => msg,
                Err(err) => {
                    warn!(
                        "WebSocket read error for peer {}: {}",
                        peer_id,
                        err
                    );
                    break;
                }
            };

            match msg {
                Message::Close(frame) => {
                    warn!(
                        "Peer {} closed websocket: {:?}",
                        peer_id,
                        frame
                    );
                    break;
                }

                Message::Ping(payload) => {
                    info!("Ping received from {}", peer_id);

                    let _ = tx_for_read.send(
                        VersionedServerMessage::new(
                            ServerMessage::HeartbeatAck,
                        ),
                    );

                    continue;
                }

                Message::Pong(_) => {
                    if let Some(mut peer) =
                        state_for_read.peers.get_mut(&peer_id)
                    {
                        peer.last_heartbeat = Instant::now();
                    }

                    continue;
                }

                Message::Binary(_) => {
                    warn!(
                        "Peer {} sent binary websocket frame",
                        peer_id
                    );

                    let _ = tx_for_read.send(
                        VersionedServerMessage::new(
                            ServerMessage::Error {
                                code: ErrorCode::MalformedMessage
                                    .as_str()
                                    .to_string(),
                                message:
                                    "Binary websocket frames are not allowed"
                                        .to_string(),
                            },
                        ),
                    );

                    break;
                }

                Message::Text(text) => {
                    if text.len()
                        > state_for_read.config.max_message_size
                    {
                        warn!(
                            "Oversized websocket message from {} ({} bytes)",
                            peer_id,
                            text.len()
                        );

                        let _ = tx_for_read.send(
                            VersionedServerMessage::new(
                                ServerMessage::Error {
                                    code: ErrorCode::RateLimitExceeded
                                        .as_str()
                                        .to_string(),
                                    message:
                                        "Message too large".to_string(),
                                },
                            ),
                        );

                        break;
                    }

                    let rate_ok = {
                        if let Some(mut peer) =
                            state_for_read.peers.get_mut(&peer_id)
                        {
                            check_rate_limit(&mut peer)
                        } else {
                            false
                        }
                    };

                    if !rate_ok {
                        warn!(
                            "Rate limit exceeded for peer {}",
                            peer_id
                        );

                        let _ = tx_for_read.send(
                            VersionedServerMessage::new(
                                ServerMessage::Error {
                                    code: ErrorCode::RateLimitExceeded
                                        .as_str()
                                        .to_string(),
                                    message:
                                        "Rate limit exceeded".to_string(),
                                },
                            ),
                        );

                        break;
                    }

                    let parsed_val: Result<serde_json::Value, _> =
                        serde_json::from_str(&text);

                    let val = match parsed_val {
                        Ok(v) => v,
                        Err(err) => {
                            warn!(
                                "Malformed JSON from {}: {}",
                                peer_id,
                                err
                            );

                            increment_bad_messages(
                                &state_for_read,
                                peer_id,
                            );

                            let _ = tx_for_read.send(
                                VersionedServerMessage::new(
                                    ServerMessage::Error {
                                        code: ErrorCode::MalformedMessage
                                            .as_str()
                                            .to_string(),
                                        message:
                                            "Invalid JSON".to_string(),
                                    },
                                ),
                            );

                            if check_bad_messages(
                                &state_for_read,
                                peer_id,
                            ) {
                                break;
                            }

                            continue;
                        }
                    };

                    let version =
                        val.get("version").and_then(|v| v.as_str());

                    if version != Some("1") {
                        warn!(
                            "Invalid protocol version from {}",
                            peer_id
                        );

                        increment_bad_messages(
                            &state_for_read,
                            peer_id,
                        );

                        let _ = tx_for_read.send(
                            VersionedServerMessage::new(
                                ServerMessage::Error {
                                    code: ErrorCode::MalformedMessage
                                        .as_str()
                                        .to_string(),
                                    message:
                                        "Unsupported protocol version"
                                            .to_string(),
                                },
                            ),
                        );

                        if check_bad_messages(
                            &state_for_read,
                            peer_id,
                        ) {
                            break;
                        }

                        continue;
                    }

                    match serde_json::from_value::<ClientMessage>(
                        val,
                    ) {
                        Ok(client_msg) => {
                            if let Err(err) =
                                crate::signaling::handle_message(
                                    &state_for_read,
                                    peer_id,
                                    client_msg,
                                )
                            {
                                warn!(
                                    "Message handling error for {}: {:?}",
                                    peer_id,
                                    err
                                );

                                let _ = tx_for_read.send(
                                    VersionedServerMessage::new(
                                        ServerMessage::Error {
                                            code: err
                                                .code
                                                .as_str()
                                                .to_string(),
                                            message: err.message,
                                        },
                                    ),
                                );
                            }
                        }

                        Err(err) => {
                            warn!(
                                "Schema validation failed from {}: {}",
                                peer_id,
                                err
                            );

                            increment_bad_messages(
                                &state_for_read,
                                peer_id,
                            );

                            let _ = tx_for_read.send(
                                VersionedServerMessage::new(
                                    ServerMessage::Error {
                                        code: ErrorCode::MalformedMessage
                                            .as_str()
                                            .to_string(),
                                        message:
                                            "Invalid schema".to_string(),
                                    },
                                ),
                            );

                            if check_bad_messages(
                                &state_for_read,
                                peer_id,
                            ) {
                                break;
                            }
                        }
                    }
                }
            }
        }

        info!("Read task ended for peer {}", peer_id);
    });

    tokio::select! {
        _ = read_task => {
            warn!("Read task exited for {}", peer_id);
        }

        _ = write_task => {
            warn!("Write task exited for {}", peer_id);
        }
    }

    info!("Cleaning up peer {}", peer_id);

    let notifications = state.remove_peer(peer_id);

    for (target_id, msg) in notifications {
        if let Some(peer) = state.peers.get(&target_id) {
            let _ = peer.tx.send(msg);
        }
    }

    info!("Cleanup completed for {}", peer_id);
}

fn check_rate_limit(peer: &mut Peer) -> bool {
    let now = Instant::now();
    let elapsed =
        now.duration_since(peer.rate_limit_last_check).as_secs();

    if elapsed > 0 {
        peer.rate_limit_tokens =
            std::cmp::min(60, peer.rate_limit_tokens + elapsed as usize);

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

fn check_bad_messages(
    state: &AppState,
    peer_id: Uuid,
) -> bool {
    if let Some(peer) = state.peers.get(&peer_id) {
        peer.bad_message_count >= 3
    } else {
        true
    }
}