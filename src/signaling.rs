use uuid::Uuid;
use tracing::{info, warn};

use crate::errors::{AppError, ErrorCode};
use crate::models::{
    ClientMessage,
    Role,
    ServerMessage,
    VersionedServerMessage,
};
use crate::security::{
    redact_ice,
    redact_sdp,
    redact_token,
};
use crate::state::AppState;

fn send_to_peer(
    state: &AppState,
    peer_id: Uuid,
    message: ServerMessage,
) -> Result<(), AppError> {
    if let Some(peer) = state.peers.get(&peer_id) {
        if peer
            .tx
            .send(VersionedServerMessage::new(message))
            .is_err()
        {
            warn!(
                "Failed to send message to peer {}",
                peer_id
            );

            return Err(AppError::new(
                ErrorCode::PeerDisconnected,
                "Peer connection closed",
            ));
        }

        Ok(())
    } else {
        Err(AppError::new(
            ErrorCode::PeerDisconnected,
            "Peer not found",
        ))
    }
}

pub fn handle_message(
    state: &AppState,
    peer_id: Uuid,
    msg: ClientMessage,
) -> Result<(), AppError> {
    match msg {
        ClientMessage::Register { role } => {
            state.register_peer(peer_id, role)?;

            send_to_peer(
                state,
                peer_id,
                ServerMessage::Registered {
                    peer_id: peer_id.to_string(),
                },
            )?;
        }

        ClientMessage::CreateSession => {
            let (transfer_id, token) =
                state.create_session(peer_id)?;

            info!(
                "Session created transfer_id={} sender={} token={}",
                transfer_id,
                peer_id,
                redact_token(&token)
            );

            send_to_peer(
                state,
                peer_id,
                ServerMessage::SessionCreated {
                    transfer_id,
                    token,
                },
            )?;
        }

        ClientMessage::JoinSession { transfer_id } => {
            let sender_id =
                state.request_join_session(peer_id, &transfer_id)?;

            info!(
                "Receiver {} requested join for {}",
                peer_id,
                transfer_id
            );

            send_to_peer(
                state,
                sender_id,
                ServerMessage::JoinRequest {
                    receiver_id: peer_id.to_string(),
                },
            )?;
        }

        ClientMessage::ApproveJoin {
            transfer_id,
            token,
            receiver_id,
        } => {
            let rx_uuid =
                Uuid::parse_str(&receiver_id).map_err(|_| {
                    AppError::new(
                        ErrorCode::MalformedMessage,
                        "Invalid receiver UUID",
                    )
                })?;

            state.approve_join_session(
                peer_id,
                &transfer_id,
                &token,
                rx_uuid,
            )?;

            info!(
                "Sender {} approved receiver {}",
                peer_id,
                rx_uuid
            );

            send_to_peer(
                state,
                rx_uuid,
                ServerMessage::SessionJoined {
                    transfer_id: transfer_id.clone(),
                },
            )?;

            send_to_peer(
                state,
                peer_id,
                ServerMessage::PeerJoined {
                    peer_id: receiver_id,
                    role: Role::Receiver,
                },
            )?;
        }

        ClientMessage::Offer {
            transfer_id,
            sdp,
        } => {
            let session =
                state.sessions.get(&transfer_id).ok_or_else(
                    || {
                        AppError::new(
                            ErrorCode::InvalidSession,
                            "Session not found",
                        )
                    },
                )?;

            let target_id =
                if session.sender_id == Some(peer_id) {
                    session.receiver_id.ok_or_else(|| {
                        AppError::new(
                            ErrorCode::PeerNotFound,
                            "Receiver missing",
                        )
                    })?
                } else if session.receiver_id
                    == Some(peer_id)
                {
                    session.sender_id.ok_or_else(|| {
                        AppError::new(
                            ErrorCode::PeerNotFound,
                            "Sender missing",
                        )
                    })?
                } else {
                    return Err(AppError::new(
                        ErrorCode::Unauthorized,
                        "Not session member",
                    ));
                };

            info!(
                "Offer relay {} -> {} SDP={}",
                peer_id,
                target_id,
                redact_sdp(&sdp)
            );

            send_to_peer(
                state,
                target_id,
                ServerMessage::RelayOffer { sdp },
            )?;
        }

        ClientMessage::Answer {
            transfer_id,
            sdp,
        } => {
            let session =
                state.sessions.get(&transfer_id).ok_or_else(
                    || {
                        AppError::new(
                            ErrorCode::InvalidSession,
                            "Session not found",
                        )
                    },
                )?;

            let target_id =
                if session.sender_id == Some(peer_id) {
                    session.receiver_id.ok_or_else(|| {
                        AppError::new(
                            ErrorCode::PeerNotFound,
                            "Receiver missing",
                        )
                    })?
                } else if session.receiver_id
                    == Some(peer_id)
                {
                    session.sender_id.ok_or_else(|| {
                        AppError::new(
                            ErrorCode::PeerNotFound,
                            "Sender missing",
                        )
                    })?
                } else {
                    return Err(AppError::new(
                        ErrorCode::Unauthorized,
                        "Not session member",
                    ));
                };

            info!(
                "Answer relay {} -> {} SDP={}",
                peer_id,
                target_id,
                redact_sdp(&sdp)
            );

            send_to_peer(
                state,
                target_id,
                ServerMessage::RelayAnswer { sdp },
            )?;
        }

        ClientMessage::IceCandidate {
            transfer_id,
            candidate,
            sdp_mid,
            sdp_mline_index,
        } => {
            if candidate.trim().is_empty() {
                return Err(AppError::new(
                    ErrorCode::MalformedMessage,
                    "Empty ICE candidate",
                ));
            }

            let session =
                state.sessions.get(&transfer_id).ok_or_else(
                    || {
                        AppError::new(
                            ErrorCode::InvalidSession,
                            "Session not found",
                        )
                    },
                )?;

            let target_id =
                if session.sender_id == Some(peer_id) {
                    session.receiver_id.ok_or_else(|| {
                        AppError::new(
                            ErrorCode::PeerNotFound,
                            "Receiver missing",
                        )
                    })?
                } else if session.receiver_id
                    == Some(peer_id)
                {
                    session.sender_id.ok_or_else(|| {
                        AppError::new(
                            ErrorCode::PeerNotFound,
                            "Sender missing",
                        )
                    })?
                } else {
                    return Err(AppError::new(
                        ErrorCode::Unauthorized,
                        "Not session member",
                    ));
                };

            info!(
                "ICE relay {} -> {} {}",
                peer_id,
                target_id,
                redact_ice(&candidate)
            );

            send_to_peer(
                state,
                target_id,
                ServerMessage::RelayIceCandidate {
                    candidate,
                    sdp_mid,
                    sdp_mline_index,
                },
            )?;
        }

        ClientMessage::Heartbeat => {
            if let Some(mut peer) =
                state.peers.get_mut(&peer_id)
            {
                peer.last_heartbeat =
                    std::time::Instant::now();
            }

            send_to_peer(
                state,
                peer_id,
                ServerMessage::HeartbeatAck,
            )?;
        }

        ClientMessage::Disconnect => {
            info!("Peer {} disconnect requested", peer_id);

            let notifications =
                state.remove_peer(peer_id);

            for (target_id, msg) in notifications {
                if let Some(peer) =
                    state.peers.get(&target_id)
                {
                    let _ = peer.tx.send(msg);
                }
            }
        }
    }

    Ok(())
}