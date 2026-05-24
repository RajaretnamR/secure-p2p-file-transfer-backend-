use uuid::Uuid;
use tracing::{info, warn};

use crate::state::AppState;
use crate::models::{ClientMessage, ServerMessage, VersionedServerMessage, Role};
use crate::errors::{AppError, ErrorCode};
use crate::security::{redact_token, redact_sdp, redact_ice};

/// Sends a versioned server message to a specific peer by ID
fn send_to_peer(state: &AppState, peer_id: Uuid, message: ServerMessage) -> Result<(), AppError> {
    if let Some(peer) = state.peers.get(&peer_id) {
        if peer.tx.send(VersionedServerMessage::new(message)).is_err() {
            warn!("Failed to send message to peer {}, channel closed", peer_id);
            return Err(AppError::new(ErrorCode::PeerDisconnected, "Peer connection closed"));
        }
        Ok(())
    } else {
        Err(AppError::new(ErrorCode::PeerDisconnected, "Peer not found"))
    }
}

/// Routes and handles messages sent by clients
pub fn handle_message(state: &AppState, peer_id: Uuid, msg: ClientMessage) -> Result<(), AppError> {
    match msg {
        ClientMessage::Register { role } => {
            state.register_peer(peer_id, role)?;
            send_to_peer(state, peer_id, ServerMessage::Registered {
                peer_id: peer_id.to_string(),
            })?;
        }

        ClientMessage::CreateSession => {
            let (transfer_id, token) = state.create_session(peer_id)?;
            info!(
                "Session created: transfer_id={}, sender={}, token={}",
                transfer_id,
                peer_id,
                redact_token(&token)
            );
            send_to_peer(state, peer_id, ServerMessage::SessionCreated {
                transfer_id,
                token,
            })?;
        }

        ClientMessage::JoinSession { transfer_id } => {
            let sender_id = state.request_join_session(peer_id, &transfer_id)?;
            info!("Relaying join request from receiver {} to sender {}", peer_id, sender_id);
            
            // Forward join request to sender. Receiver will wait for approval.
            send_to_peer(state, sender_id, ServerMessage::JoinRequest {
                receiver_id: peer_id.to_string(),
            })?;
        }

        ClientMessage::ApproveJoin { transfer_id, token, receiver_id } => {
            let rx_uuid = Uuid::parse_str(&receiver_id).map_err(|_| {
                AppError::new(ErrorCode::MalformedMessage, "Invalid receiver UUID format")
            })?;

            state.approve_join_session(peer_id, &transfer_id, &token, rx_uuid)?;
            info!("Join approved for receiver {} by sender {}", rx_uuid, peer_id);

            // Notify receiver that session is joined
            send_to_peer(state, rx_uuid, ServerMessage::SessionJoined {
                transfer_id: transfer_id.clone(),
            })?;

            // Confirm join to sender
            send_to_peer(state, peer_id, ServerMessage::PeerJoined {
                peer_id: receiver_id,
                role: Role::Receiver,
            })?;
        }

        ClientMessage::Offer { transfer_id, sdp } => {
            let session = state.sessions.get(&transfer_id).ok_or_else(|| {
                AppError::new(ErrorCode::InvalidSession, "Session does not exist")
            })?;

            // Anti-spoofing / Membership check
            let target_id = if Some(peer_id) == session.sender_id {
                session.receiver_id.ok_or_else(|| {
                    AppError::new(ErrorCode::PeerNotFound, "Receiver not connected to session")
                })?
            } else if Some(peer_id) == session.receiver_id {
                session.sender_id.ok_or_else(|| {
                    AppError::new(ErrorCode::PeerNotFound, "Sender not connected to session")
                })?
            } else {
                return Err(AppError::new(ErrorCode::Unauthorized, "Not a member of this session"));
            };

            info!(
                "Relaying offer from {} to {} in session {}. SDP: {}",
                peer_id,
                target_id,
                transfer_id,
                redact_sdp(&sdp)
            );

            send_to_peer(state, target_id, ServerMessage::RelayOffer { sdp })?;
        }

        ClientMessage::Answer { transfer_id, sdp } => {
            let session = state.sessions.get(&transfer_id).ok_or_else(|| {
                AppError::new(ErrorCode::InvalidSession, "Session does not exist")
            })?;

            // Anti-spoofing / Membership check
            let target_id = if Some(peer_id) == session.sender_id {
                session.receiver_id.ok_or_else(|| {
                    AppError::new(ErrorCode::PeerNotFound, "Receiver not connected to session")
                })?
            } else if Some(peer_id) == session.receiver_id {
                session.sender_id.ok_or_else(|| {
                    AppError::new(ErrorCode::PeerNotFound, "Sender not connected to session")
                })?
            } else {
                return Err(AppError::new(ErrorCode::Unauthorized, "Not a member of this session"));
            };

            info!(
                "Relaying answer from {} to {} in session {}. SDP: {}",
                peer_id,
                target_id,
                transfer_id,
                redact_sdp(&sdp)
            );

            send_to_peer(state, target_id, ServerMessage::RelayAnswer { sdp })?;
        }

        ClientMessage::IceCandidate { transfer_id, candidate, sdp_mid, sdp_mline_index } => {
            // Schema Validation: Ensure candidate string is not empty
            if candidate.trim().is_empty() {
                return Err(AppError::new(ErrorCode::MalformedMessage, "Candidate string cannot be empty"));
            }

            let session = state.sessions.get(&transfer_id).ok_or_else(|| {
                AppError::new(ErrorCode::InvalidSession, "Session does not exist")
            })?;

            // Anti-spoofing / Membership check
            let target_id = if Some(peer_id) == session.sender_id {
                session.receiver_id.ok_or_else(|| {
                    AppError::new(ErrorCode::PeerNotFound, "Receiver not connected to session")
                })?
            } else if Some(peer_id) == session.receiver_id {
                session.sender_id.ok_or_else(|| {
                    AppError::new(ErrorCode::PeerNotFound, "Sender not connected to session")
                })?
            } else {
                return Err(AppError::new(ErrorCode::Unauthorized, "Not a member of this session"));
            };

            info!(
                "Relaying ICE candidate from {} to {} in session {}. Candidate: {}",
                peer_id,
                target_id,
                transfer_id,
                redact_ice(&candidate)
            );

            send_to_peer(state, target_id, ServerMessage::RelayIceCandidate {
                candidate,
                sdp_mid,
                sdp_mline_index,
            })?;
        }

        ClientMessage::Heartbeat => {
            if let Some(mut peer) = state.peers.get_mut(&peer_id) {
                peer.last_heartbeat = std::time::Instant::now();
            }
            send_to_peer(state, peer_id, ServerMessage::HeartbeatAck)?;
        }

        ClientMessage::Disconnect => {
            info!("Peer {} sent disconnect message. Tearing down.", peer_id);
            let notifications = state.remove_peer(peer_id);
            for (target_id, msg) in notifications {
                if let Some(peer) = state.peers.get(&target_id) {
                    let _ = peer.tx.send(msg);
                }
            }
        }
    }
    Ok(())
}
