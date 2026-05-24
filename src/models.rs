use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Sender,
    Receiver,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ClientMessage {
    Register {
        role: Role,
    },
    CreateSession,
    JoinSession {
        #[serde(rename = "transferId")]
        transfer_id: String,
    },
    ApproveJoin {
        #[serde(rename = "transferId")]
        transfer_id: String,
        token: String,
        #[serde(rename = "receiverId")]
        receiver_id: String,
    },
    Offer {
        #[serde(rename = "transferId")]
        transfer_id: String,
        sdp: String,
    },
    Answer {
        #[serde(rename = "transferId")]
        transfer_id: String,
        sdp: String,
    },
    IceCandidate {
        #[serde(rename = "transferId")]
        transfer_id: String,
        candidate: String,
        #[serde(rename = "sdpMid")]
        sdp_mid: Option<String>,
        #[serde(rename = "sdpMLineIndex")]
        sdp_mline_index: Option<u16>,
    },
    Heartbeat,
    Disconnect,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ServerMessage {
    Registered {
        #[serde(rename = "peerId")]
        peer_id: String,
    },
    SessionCreated {
        #[serde(rename = "transferId")]
        transfer_id: String,
        token: String,
    },
    JoinRequest {
        #[serde(rename = "receiverId")]
        receiver_id: String,
    },
    SessionJoined {
        #[serde(rename = "transferId")]
        transfer_id: String,
    },
    PeerJoined {
        #[serde(rename = "peerId")]
        peer_id: String,
        role: Role,
    },
    PeerDisconnected {
        #[serde(rename = "peerId")]
        peer_id: String,
        role: Role,
    },
    RelayOffer {
        sdp: String,
    },
    RelayAnswer {
        sdp: String,
    },
    RelayIceCandidate {
        candidate: String,
        #[serde(rename = "sdpMid")]
        sdp_mid: Option<String>,
        #[serde(rename = "sdpMLineIndex")]
        sdp_mline_index: Option<u16>,
    },
    HeartbeatAck,
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedServerMessage {
    pub version: String,
    #[serde(flatten)]
    pub payload: ServerMessage,
}

impl VersionedServerMessage {
    pub fn new(payload: ServerMessage) -> Self {
        Self {
            version: "1".to_string(),
            payload,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            r#"{"version":"1","type":"error","code":"INTERNAL_ERROR","message":"Failed to serialize message"}"#.to_string()
        })
    }
}
