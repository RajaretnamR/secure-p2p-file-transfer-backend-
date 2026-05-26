use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};
use uuid::Uuid;

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
    pub fn new(
        id: Uuid,
        tx: UnboundedSender<VersionedServerMessage>,
    ) -> Self {
        Self {
            id,
            role: None,
            tx,
            last_heartbeat: Instant::now(),
            session_id: None,
            rate_limit_tokens: 60,
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

    pub fn add_peer(
        &self,
        id: Uuid,
        tx: UnboundedSender<VersionedServerMessage>,
    ) -> Result<(), AppError> {
        if self.peers.len() >= self.config.max_connections {
            return Err(AppError::new(
                ErrorCode::RateLimitExceeded,
                "Server connection cap reached",
            ));
        }

        self.peers.insert(id, Peer::new(id, tx));

        info!("Peer added: {}", id);

        Ok(())
    }

    pub fn register_peer(
        &self,
        peer_id: Uuid,
        role: Role,
    ) -> Result<(), AppError> {
        let mut peer = self.peers.get_mut(&peer_id).ok_or_else(|| {
            AppError::new(
                ErrorCode::NotRegistered,
                "Peer not found",
            )
        })?;

        if peer.role.is_some() {
            return Err(AppError::new(
                ErrorCode::AlreadyRegistered,
                "Peer already registered",
            ));
        }

        peer.role = Some(role);

        info!("Peer {} registered as {:?}", peer_id, role);

        Ok(())
    }

    pub fn create_session(
        &self,
        sender_id: Uuid,
    ) -> Result<(String, String), AppError> {
        let mut sender_peer =
            self.peers.get_mut(&sender_id).ok_or_else(|| {
                AppError::new(
                    ErrorCode::NotRegistered,
                    "Sender not found",
                )
            })?;

        if sender_peer.role != Some(Role::Sender) {
            return Err(AppError::new(
                ErrorCode::InvalidRole,
                "Only senders can create sessions",
            ));
        }

        if sender_peer.session_id.is_some() {
            return Err(AppError::new(
                ErrorCode::AlreadyRegistered,
                "Sender already has active session",
            ));
        }

        let transfer_id =
            crate::security::generate_transfer_id();

        let token =
            crate::security::generate_secure_token();

        let now = Instant::now();

        let expires_at = now
            + Duration::from_secs(
                self.config.session_timeout_minutes * 60,
            );

        let session = Session {
            transfer_id: transfer_id.clone(),
            token: token.clone(),
            sender_id: Some(sender_id),
            receiver_id: None,
            created_at: now,
            expires_at,
        };

        self.sessions.insert(
            transfer_id.clone(),
            session,
        );

        sender_peer.session_id =
            Some(transfer_id.clone());

        info!(
            "Session created: {} by {}",
            transfer_id,
            sender_id
        );

        Ok((transfer_id, token))
    }

    pub fn request_join_session(
        &self,
        receiver_id: Uuid,
        transfer_id: &str,
    ) -> Result<Uuid, AppError> {
        let mut receiver_peer =
            self.peers.get_mut(&receiver_id).ok_or_else(|| {
                AppError::new(
                    ErrorCode::NotRegistered,
                    "Receiver not found",
                )
            })?;

        if receiver_peer.role != Some(Role::Receiver) {
            return Err(AppError::new(
                ErrorCode::InvalidRole,
                "Only receivers can join sessions",
            ));
        }

        if receiver_peer.session_id.is_some() {
            return Err(AppError::new(
                ErrorCode::AlreadyRegistered,
                "Receiver already in session",
            ));
        }

        let session =
            self.sessions.get(transfer_id).ok_or_else(|| {
                AppError::new(
                    ErrorCode::InvalidSession,
                    "Session not found",
                )
            })?;

        if Instant::now() > session.expires_at {
            return Err(AppError::new(
                ErrorCode::SessionExpired,
                "Session expired",
            ));
        }

        if session.receiver_id.is_some() {
            return Err(AppError::new(
                ErrorCode::SessionFull,
                "Session already occupied",
            ));
        }

        let sender_id =
            session.sender_id.ok_or_else(|| {
                AppError::new(
                    ErrorCode::PeerDisconnected,
                    "Sender disconnected",
                )
            })?;

        receiver_peer.session_id =
            Some(transfer_id.to_string());

        info!(
            "Receiver {} requested join for {}",
            receiver_id,
            transfer_id
        );

        Ok(sender_id)
    }

    pub fn approve_join_session(
        &self,
        sender_id: Uuid,
        transfer_id: &str,
        token: &str,
        receiver_id: Uuid,
    ) -> Result<(), AppError> {
        let sender_peer =
            self.peers.get(&sender_id).ok_or_else(|| {
                AppError::new(
                    ErrorCode::NotRegistered,
                    "Sender not found",
                )
            })?;

        if sender_peer.role != Some(Role::Sender) {
            return Err(AppError::new(
                ErrorCode::InvalidRole,
                "Only senders can approve joins",
            ));
        }

        let mut session =
            self.sessions.get_mut(transfer_id).ok_or_else(
                || {
                    AppError::new(
                        ErrorCode::InvalidSession,
                        "Session not found",
                    )
                },
            )?;

        if session.token != token {
            return Err(AppError::new(
                ErrorCode::Unauthorized,
                "Invalid token",
            ));
        }

        if session.sender_id != Some(sender_id) {
            return Err(AppError::new(
                ErrorCode::Unauthorized,
                "Not session owner",
            ));
        }

        if Instant::now() > session.expires_at {
            return Err(AppError::new(
                ErrorCode::SessionExpired,
                "Session expired",
            ));
        }

        let receiver_peer =
            self.peers.get(&receiver_id).ok_or_else(|| {
                AppError::new(
                    ErrorCode::PeerDisconnected,
                    "Receiver disconnected",
                )
            })?;

        if receiver_peer.role != Some(Role::Receiver) {
            return Err(AppError::new(
                ErrorCode::InvalidRole,
                "Target not receiver",
            ));
        }

        session.receiver_id = Some(receiver_id);

        info!(
            "Sender {} approved receiver {}",
            sender_id,
            receiver_id
        );

        Ok(())
    }

    pub fn remove_peer(
        &self,
        peer_id: Uuid,
    ) -> Vec<(Uuid, VersionedServerMessage)> {
        let mut notifications = Vec::new();

        if let Some((_, peer)) = self.peers.remove(&peer_id) {
            info!("Removing peer {}", peer_id);

            if let Some(transfer_id) = peer.session_id {
                if let Some(mut session) =
                    self.sessions.get_mut(&transfer_id)
                {
                    if session.sender_id == Some(peer_id) {
                        if let Some(receiver_id) =
                            session.receiver_id
                        {
                            if let Some(mut rec_peer) =
                                self.peers.get_mut(&receiver_id)
                            {
                                rec_peer.session_id = None;
                            }

                            notifications.push((
                                receiver_id,
                                VersionedServerMessage::new(
                                    ServerMessage::PeerDisconnected {
                                        peer_id:
                                            peer_id.to_string(),
                                        role: Role::Sender,
                                    },
                                ),
                            ));
                        }

                        self.sessions.remove(&transfer_id);

                        info!(
                            "Sender disconnected. Session deleted {}",
                            transfer_id
                        );
                    } else if session.receiver_id
                        == Some(peer_id)
                    {
                        session.receiver_id = None;

                        if let Some(sender_id) =
                            session.sender_id
                        {
                            if let Some(mut sender_peer) =
                                self.peers.get_mut(&sender_id)
                            {
                                sender_peer.session_id =
                                    Some(
                                        transfer_id.clone(),
                                    );
                            }

                            notifications.push((
                                sender_id,
                                VersionedServerMessage::new(
                                    ServerMessage::PeerDisconnected {
                                        peer_id:
                                            peer_id.to_string(),
                                        role: Role::Receiver,
                                    },
                                ),
                            ));
                        }

                        info!(
                            "Receiver disconnected from {}",
                            transfer_id
                        );
                    }
                }
            }
        }

        notifications
    }

    pub fn cleanup_stale(
        &self,
    ) -> Vec<(Uuid, VersionedServerMessage)> {
        let mut notifications = Vec::new();

        let now = Instant::now();

        let mut dead_peers = Vec::new();

        for peer in self.peers.iter() {
            if peer.last_heartbeat.elapsed().as_secs()
                > self.config.heartbeat_timeout_seconds
            {
                dead_peers.push(peer.id);
            }
        }

        for peer_id in dead_peers {
            warn!("Heartbeat timeout for {}", peer_id);

            let mut notes = self.remove_peer(peer_id);

            notifications.append(&mut notes);
        }

        let mut expired_sessions = Vec::new();

        for session in self.sessions.iter() {
            if now > session.expires_at {
                expired_sessions
                    .push(session.transfer_id.clone());
            }
        }

        for transfer_id in expired_sessions {
            if let Some((_, session)) =
                self.sessions.remove(&transfer_id)
            {
                if let Some(sender_id) = session.sender_id {
                    if let Some(mut peer) =
                        self.peers.get_mut(&sender_id)
                    {
                        peer.session_id = None;
                    }
                }

                if let Some(receiver_id) =
                    session.receiver_id
                {
                    if let Some(mut peer) =
                        self.peers.get_mut(&receiver_id)
                    {
                        peer.session_id = None;
                    }
                }

                info!(
                    "Expired session removed {}",
                    transfer_id
                );
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