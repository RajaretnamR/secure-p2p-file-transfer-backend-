#[cfg(test)]
mod test_suite {
    use std::time::{Duration, Instant};
    use uuid::Uuid;
    use tokio::sync::mpsc;

    use crate::config::Config;
    use crate::state::AppState;
    use crate::models::{Role, ClientMessage, ServerMessage, VersionedServerMessage};
    use crate::errors::ErrorCode;
    use crate::signaling::handle_message;

    // Helper to create a test AppState with custom intervals
    fn setup_test_state() -> AppState {
        let mut config = Config::load();
        config.max_connections = 5;
        config.session_timeout_minutes = 1;
        config.heartbeat_timeout_seconds = 2; // Short heartbeat timeout for testing
        AppState::new(config)
    }

    // Helper to create a registered peer in the state
    fn create_registered_peer(state: &AppState, role: Role) -> (Uuid, mpsc::UnboundedReceiver<VersionedServerMessage>) {
        let peer_id = Uuid::new_v4();
        let (tx, rx) = mpsc::unbounded_channel();
        state.add_peer(peer_id, tx).unwrap();
        state.register_peer(peer_id, role).unwrap();
        (peer_id, rx)
    }

    #[test]
    fn test_sender_registration() {
        let state = setup_test_state();
        let peer_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        
        // Add peer
        assert!(state.add_peer(peer_id, tx).is_ok());
        assert!(state.peers.contains_key(&peer_id));

        // Register role
        assert!(state.register_peer(peer_id, Role::Sender).is_ok());
        let peer = state.peers.get(&peer_id).unwrap();
        assert_eq!(peer.role, Some(Role::Sender));
    }

    #[test]
    fn test_duplicate_registration() {
        let state = setup_test_state();
        let (peer_id, _rx) = create_registered_peer(&state, Role::Sender);

        // Attempt second registration
        let res = state.register_peer(peer_id, Role::Receiver);
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code, ErrorCode::AlreadyRegistered);
    }

    #[test]
    fn test_session_creation() {
        let state = setup_test_state();
        let (sender_id, _rx) = create_registered_peer(&state, Role::Sender);

        // Create session
        let res = state.create_session(sender_id);
        assert!(res.is_ok());
        let (transfer_id, token) = res.unwrap();
        
        // Verification of lengths and structure
        assert_eq!(transfer_id.len(), 10);
        assert_eq!(token.len(), 32);
        assert!(state.sessions.contains_key(&transfer_id));

        let session = state.sessions.get(&transfer_id).unwrap();
        assert_eq!(session.sender_id, Some(sender_id));
        assert_eq!(session.receiver_id, None);
    }

    #[test]
    fn test_invalid_join() {
        let state = setup_test_state();
        let (receiver_id, _rx) = create_registered_peer(&state, Role::Receiver);

        // Join non-existent session
        let res = state.request_join_session(receiver_id, "NONEXISTENT");
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code, ErrorCode::InvalidSession);
    }

    #[test]
    fn test_approve_join_success() {
        let state = setup_test_state();
        let (sender_id, _sender_rx) = create_registered_peer(&state, Role::Sender);
        let (receiver_id, _receiver_rx) = create_registered_peer(&state, Role::Receiver);

        // 1. Create session
        let (transfer_id, token) = state.create_session(sender_id).unwrap();

        // 2. Request join (returns sender UUID)
        let sender_uuid = state.request_join_session(receiver_id, &transfer_id).unwrap();
        assert_eq!(sender_uuid, sender_id);

        // 3. Approve join
        let res = state.approve_join_session(sender_id, &transfer_id, &token, receiver_id);
        assert!(res.is_ok());

        // Verify session mappings in state
        let session = state.sessions.get(&transfer_id).unwrap();
        assert_eq!(session.receiver_id, Some(receiver_id));
        assert_eq!(session.sender_id, Some(sender_id));

        assert_eq!(state.peers.get(&sender_id).unwrap().session_id.as_deref(), Some(transfer_id.as_str()));
        assert_eq!(state.peers.get(&receiver_id).unwrap().session_id.as_deref(), Some(transfer_id.as_str()));
    }

    #[test]
    fn test_unauthorized_offer_answer() {
        let state = setup_test_state();
        let (sender_id, _sender_rx) = create_registered_peer(&state, Role::Sender);
        let (receiver_id, _receiver_rx) = create_registered_peer(&state, Role::Receiver);
        let (outsider_id, _outsider_rx) = create_registered_peer(&state, Role::Receiver);

        let (transfer_id, token) = state.create_session(sender_id).unwrap();
        state.request_join_session(receiver_id, &transfer_id).unwrap();
        state.approve_join_session(sender_id, &transfer_id, &token, receiver_id).unwrap();

        // Outsider attempts to relay offer
        let res = handle_message(&state, outsider_id, ClientMessage::Offer {
            transfer_id: transfer_id.clone(),
            sdp: "fake sdp".to_string(),
        });
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code, ErrorCode::Unauthorized);
    }

    #[test]
    fn test_expired_session() {
        let state = setup_test_state();
        let (sender_id, _sender_rx) = create_registered_peer(&state, Role::Sender);
        
        let (transfer_id, _token) = state.create_session(sender_id).unwrap();

        // Force session expiration
        {
            let mut session = state.sessions.get_mut(&transfer_id).unwrap();
            session.expires_at = Instant::now() - Duration::from_secs(1);
        }

        // Run cleanup
        let notifications = state.cleanup_stale();
        
        // Session should be removed
        assert!(!state.sessions.contains_key(&transfer_id));
        assert!(state.peers.get(&sender_id).unwrap().session_id.is_none());

        // Notifications should contain session expired message to sender
        assert_eq!(notifications.len(), 1);
        let (target, ref msg) = notifications[0];
        assert_eq!(target, sender_id);
        if let ServerMessage::Error { ref code, .. } = msg.payload {
            assert_eq!(code, "SESSION_EXPIRED");
        } else {
            panic!("Expected Error message");
        }
    }

    #[test]
    fn test_heartbeat_timeout() {
        let state = setup_test_state();
        let (sender_id, _sender_rx) = create_registered_peer(&state, Role::Sender);

        // Advance peer heartbeat timestamp back in time
        {
            let mut peer = state.peers.get_mut(&sender_id).unwrap();
            peer.last_heartbeat = Instant::now() - Duration::from_secs(5);
        }

        // Run cleanup
        let _notifications = state.cleanup_stale();

        // Peer should be removed due to timeout
        assert!(!state.peers.contains_key(&sender_id));
    }

    #[test]
    fn test_disconnect_cleanup() {
        let state = setup_test_state();
        let (sender_id, _sender_rx) = create_registered_peer(&state, Role::Sender);
        let (receiver_id, _receiver_rx) = create_registered_peer(&state, Role::Receiver);

        let (transfer_id, token) = state.create_session(sender_id).unwrap();
        state.request_join_session(receiver_id, &transfer_id).unwrap();
        state.approve_join_session(sender_id, &transfer_id, &token, receiver_id).unwrap();

        // 1. Disconnect Receiver
        let notifications = state.remove_peer(receiver_id);
        
        // Session must still exist (sender can wait for reconnect)
        assert!(state.sessions.contains_key(&transfer_id));
        {
            let session = state.sessions.get(&transfer_id).unwrap();
            assert_eq!(session.receiver_id, None);
            assert_eq!(session.sender_id, Some(sender_id));
        }

        // Sender should have received disconnect notification
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].0, sender_id);
        assert!(matches!(notifications[0].1.payload, ServerMessage::PeerDisconnected { .. }));

        // 2. Disconnect Sender
        let notifications = state.remove_peer(sender_id);
        
        // Session must be completely deleted
        assert!(!state.sessions.contains_key(&transfer_id));
        assert!(notifications.is_empty()); // receiver is already disconnected
    }
}
