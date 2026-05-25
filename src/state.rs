use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;
use uuid::Uuid;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

use crate::config::Config;
use crate::errors::{AppError, ErrorCode};
use crate::models::{Role, ServerMessage, VersionedServerMessage};

#[derive(Clone)]
pub struct Peer {
    pub id: Uuid,
    pub role: Option<Role>,
    pub tx: UnboundedSender<VersionedServerMessage>,
    pub last_heartbeat: Instant,
    pub session_id: Option<String>,
    pub rate_limit_tokens: usize,
    pub rate_limit_last_check: Instant,
    pub bad_message_count: usize,
}

impl Peer {
    pub fn new(id: Uuid, tx: UnboundedSender<VersionedServerMessage>) -> Self {
        Self {
            id,
            role: None,
            tx,
            last_heartbeat: Instant::now(),
            session_id: None,
            rate_limit_tokens: 30, // Max 30 messages burst
            rate_limit_last_check: Instant::now(),
            bad_message_count: 0,
        }
    }
}

pub struct Session {
    pub transfer_id: String,
    pub token: String,
    pub sender_id: Option<Uuid>,
    pub receiver_id: Option<Uuid>,
    #[allow(dead_code)]
    pub created_at: Instant,
    pub expires_at: Instant,
}

#[derive(Clone)]
pub struct AppState {
    pub peers: Arc<DashMap<Uuid, Peer>>,
    pub sessions: Arc<DashMap<String, Session>>,
    pub config: Config,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        Self {
            peers: Arc::new(DashMap::new()),
            sessions: Arc::new(DashMap::new()),
            config,
        }
    }

    /// Add a new connection peer to the registry
    pub fn add_peer(&self, id: Uuid, tx: UnboundedSender<VersionedServerMessage>) -> Result<(), AppError> {
        if self.peers.len() >= self.config.max_connections {
            warn!("Rejected new connection. Connection cap of {} reached.", self.config.max_connections);
            return Err(AppError::new(ErrorCode::RateLimitExceeded, "Server connection cap reached"));
        }
        self.peers.insert(id, Peer::new(id, tx));
        info!("Peer added to registry: {}", id);
        Ok(())
    }

    /// Register role for a peer
    pub fn register_peer(&self, peer_id: Uuid, role: Role) -> Result<(), AppError> {
        let mut peer = self.peers.get_mut(&peer_id).ok_or_else(|| {
            AppError::new(ErrorCode::NotRegistered, "Connection peer not found")
        })?;

        if peer.role.is_some() {
            return Err(AppError::new(ErrorCode::AlreadyRegistered, "Peer is already registered"));
        }

        peer.role = Some(role);
        info!("Peer {} registered as {:?}", peer_id, role);
        Ok(())
    }

    /// Create session for a sender
pub fn create_session(&self, sender_id: Uuid) -> Result<(String, String), AppError> {
    let mut sender_peer = self.peers.get_mut(&sender_id).ok_or_else(|| {
        AppError::new(ErrorCode::NotRegistered, "Peer not registered")
    })?;

    if sender_peer.role != Some(Role::Sender) {
        return Err(AppError::new(
            ErrorCode::InvalidRole,
            "Only registered Senders can create sessions",
        ));
    }

    // reconnect recovery
    for mut session in self.sessions.iter_mut() {
        if session.sender_id.is_none() && session.receiver_id.is_none() {
            session.sender_id = Some(sender_id);
            sender_peer.session_id = Some(session.transfer_id.clone());

            info!(
                "Recovered orphan session {} for sender {}",
                session.transfer_id,
                sender_id
            );

            return Ok((
                session.transfer_id.clone(),
                session.token.clone(),
            ));
        }
    }

    if sender_peer.session_id.is_some() {
        return Err(AppError::new(
            ErrorCode::AlreadyRegistered,
            "Sender already has an active session",
        ));
    }

    let transfer_id = crate::security::generate_transfer_id();
    let token = crate::security::generate_secure_token();

    let now = Instant::now();
    let expires_at =
        now + Duration::from_secs(self.config.session_timeout_minutes * 60);

    let session = Session {
        transfer_id: transfer_id.clone(),
        token: token.clone(),
        sender_id: Some(sender_id),
        receiver_id: None,
        created_at: now,
        expires_at,
    };

    self.sessions.insert(transfer_id.clone(), session);
    sender_peer.session_id = Some(transfer_id.clone());

    info!(
        "Session created. transfer_id={}, sender_id={}",
        transfer_id,
        sender_id
    );

    Ok((transfer_id, token))
}

    /// Receiver requests to join a session. Returns the Sender's UUID to notify them.
    pub fn request_join_session(&self, receiver_id: Uuid, transfer_id: &str) -> Result<Uuid, AppError> {
        let receiver_peer = self.peers.get(&receiver_id).ok_or_else(|| {
            AppError::new(ErrorCode::NotRegistered, "Peer not registered")
        })?;

        if receiver_peer.role != Some(Role::Receiver) {
            return Err(AppError::new(ErrorCode::InvalidRole, "Only registered Receivers can join sessions"));
        }

        if receiver_peer.session_id.is_some() {
            return Err(AppError::new(ErrorCode::AlreadyRegistered, "Receiver is already in an active session"));
        }

        let session = self.sessions.get(transfer_id).ok_or_else(|| {
            AppError::new(ErrorCode::InvalidSession, "Session does not exist")
        })?;

        if Instant::now() > session.expires_at {
            return Err(AppError::new(ErrorCode::SessionExpired, "Session has expired"));
        }

        if session.receiver_id.is_some() {
            return Err(AppError::new(ErrorCode::SessionFull, "Session already has an active receiver"));
        }

        let sender_id = session.sender_id.ok_or_else(|| {
            AppError::new(ErrorCode::InvalidSession, "Session owner has disconnected")
        })?;

        info!("Join request received for transfer_id={} from receiver_id={}", transfer_id, receiver_id);
        Ok(sender_id)
    }

    /// Sender approves the join request
    pub fn approve_join_session(&self, sender_id: Uuid, transfer_id: &str, token: &str, receiver_id: Uuid) -> Result<(), AppError> {
        let sender_peer = self.peers.get(&sender_id).ok_or_else(|| {
            AppError::new(ErrorCode::NotRegistered, "Sender peer not found")
        })?;

        if sender_peer.role != Some(Role::Sender) {
            return Err(AppError::new(ErrorCode::InvalidRole, "Only Senders can approve joins"));
        }

        let mut session = self.sessions.get_mut(transfer_id).ok_or_else(|| {
            AppError::new(ErrorCode::InvalidSession, "Session does not exist")
        })?;

        if session.token != token {
            return Err(AppError::new(ErrorCode::Unauthorized, "Invalid session token"));
        }

        if session.sender_id != Some(sender_id) {
            return Err(AppError::new(ErrorCode::Unauthorized, "You are not the owner of this session"));
        }

        if Instant::now() > session.expires_at {
            return Err(AppError::new(ErrorCode::SessionExpired, "Session has expired"));
        }

        if session.receiver_id.is_some() {
            return Err(AppError::new(ErrorCode::SessionFull, "Session already has an active receiver"));
        }

        // Validate receiver still exists and is registered as receiver
        let mut receiver_peer = self.peers.get_mut(&receiver_id).ok_or_else(|| {
            AppError::new(ErrorCode::NotRegistered, "Receiver has disconnected")
        })?;

        if receiver_peer.role != Some(Role::Receiver) {
            return Err(AppError::new(ErrorCode::InvalidRole, "Target peer is not a registered Receiver"));
        }

        session.receiver_id = Some(receiver_id);
        receiver_peer.session_id = Some(transfer_id.to_string());

        info!("Join approved for transfer_id={}, receiver_id={}", transfer_id, receiver_id);
        Ok(())
    }

    /// Handles disconnection of a peer, cleaning up references and returning pending notifications
pub fn remove_peer(&self, peer_id: Uuid) -> Vec<(Uuid, VersionedServerMessage)> {
    let mut notifications = Vec::new();

    if let Some((_, peer)) = self.peers.remove(&peer_id) {
        info!("Peer removed: {}", peer_id);

        if let Some(transfer_id) = peer.session_id {
            if let Some(mut session) = self.sessions.get_mut(&transfer_id) {

                // SENDER DISCONNECTED
                if Some(peer_id) == session.sender_id {
                    session.sender_id = None;

                    if let Some(receiver_id) = session.receiver_id {
                        if let Some(mut rec_peer) = self.peers.get_mut(&receiver_id) {
                            rec_peer.session_id = None;
                        }

                        notifications.push((
                            receiver_id,
                            VersionedServerMessage::new(ServerMessage::PeerDisconnected {
                                peer_id: peer_id.to_string(),
                                role: Role::Sender,
                            }),
                        ));
                    }

                    info!(
                        "Sender {} disconnected temporarily. Session {} kept alive for reconnect.",
                        peer_id,
                        transfer_id
                    );
                }

                // RECEIVER DISCONNECTED
                else if Some(peer_id) == session.receiver_id {
                    session.receiver_id = None;

                    if let Some(sender_id) = session.sender_id {
                        notifications.push((
                            sender_id,
                            VersionedServerMessage::new(ServerMessage::PeerDisconnected {
                                peer_id: peer_id.to_string(),
                                role: Role::Receiver,
                            }),
                        ));
                    }

                    info!(
                        "Receiver disconnected from session {}. Session remains active.",
                        transfer_id
                    );
                }
            }
        }
    }

    notifications
}

    /// Background task cleanup: purge stale sessions and inactive peers
    pub fn cleanup_stale(&self) -> Vec<(Uuid, VersionedServerMessage)> {
        let mut notifications = Vec::new();
        let now = Instant::now();

        // 1. Inactive peers
        let mut dead_peer_ids = Vec::new();
        for entry in self.peers.iter() {
            let peer = entry.value();
            if peer.last_heartbeat.elapsed().as_secs() > self.config.heartbeat_timeout_seconds {
                dead_peer_ids.push(peer.id);
            }
        }

        for peer_id in dead_peer_ids {
            warn!("Peer {} timed out (heartbeat missing). Cleaning up.", peer_id);
            let mut notes = self.remove_peer(peer_id);
            notifications.append(&mut notes);
        }

        // 2. Expired sessions
        let mut expired_session_ids = Vec::new();
        for entry in self.sessions.iter() {
            let session = entry.value();
            if now >= session.expires_at {
                expired_session_ids.push(session.transfer_id.clone());
            }
        }

        for transfer_id in expired_session_ids {
            if let Some((_, session)) = self.sessions.remove(&transfer_id) {
                info!("Session {} has expired.", transfer_id);
                if let Some(sender_id) = session.sender_id {
                    if let Some(mut sender_peer) = self.peers.get_mut(&sender_id) {
                        sender_peer.session_id = None;
                    }
                    notifications.push((
                        sender_id,
                        VersionedServerMessage::new(ServerMessage::Error {
                            code: ErrorCode::SessionExpired.as_str().to_string(),
                            message: "Session has expired due to timeout".to_string(),
                        }),
                    ));
                }
                if let Some(receiver_id) = session.receiver_id {
                    if let Some(mut receiver_peer) = self.peers.get_mut(&receiver_id) {
                        receiver_peer.session_id = None;
                    }
                    notifications.push((
                        receiver_id,
                        VersionedServerMessage::new(ServerMessage::Error {
                            code: ErrorCode::SessionExpired.as_str().to_string(),
                            message: "Session has expired due to timeout".to_string(),
                        }),
                    ));
                }
            }
        }

        notifications
    }
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::NotRegistered => "NOT_REGISTERED",
            ErrorCode::AlreadyRegistered => "ALREADY_REGISTERED",
            ErrorCode::InvalidRole => "INVALID_ROLE",
            ErrorCode::SessionExpired => "SESSION_EXPIRED",
            ErrorCode::SessionFull => "SESSION_FULL",
            ErrorCode::InvalidSession => "INVALID_SESSION",
            ErrorCode::Unauthorized => "UNAUTHORIZED",
            ErrorCode::PeerDisconnected => "PEER_DISCONNECTED",
            ErrorCode::PeerNotFound => "PEER_NOT_FOUND",
            ErrorCode::MalformedMessage => "MALFORMED_MESSAGE",
            ErrorCode::InternalError => "INTERNAL_ERROR",
            ErrorCode::RateLimitExceeded => "RATE_LIMIT_EXCEEDED",
        }
    }
}
