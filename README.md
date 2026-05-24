# WebRTC signaling backend server

A production-grade Rust-based signaling backend built using Axum, Tokio, and WebSockets for a Peer-to-Peer WebRTC file transfer system.

## Architectural Design

This backend acts **strictly as a signaling infrastructure** and **never relays binary file data**. Its sole responsibilities are:
- Peer registration and connection management.
- Secure, high-entropy transfer session management.
- WebRTC SDP offer/answer and ICE candidate relay.

After WebRTC signaling is complete, the connected browsers communicate directly using standard RTCPeerConnection, keeping the server out of the high-bandwidth data path.

### Core Security Policies
1. **Join Request Approval**: Senders generate a transfer session resulting in a readable 10-character alphanumeric `transferId` (e.g. `X7K9Q2LM`) and a secure 32-character `token`. The receiver requests to join using only the `transferId`. The sender is prompted on their WebSocket connection with the receiver's details, and must explicitly approve the join request by replying with their secret `token`.
2. **Binary Message Rejection**: Immediate WebSocket termination with code `1003 (Unsupported Data)` if a binary websocket frame is received.
3. **Max Payload Cap**: Payloads exceeding 64KB are rejected at the connection layer to protect server memory.
4. **Flood Protection / Rate Limiting**: Per-connection token-bucket rate limiter limiting clients to 30 messages/minute.
5. **Abuse Protection**: Connections are terminated if a client sends 3 or more malformed or unauthorized messages.
6. **Logging Redaction**: Logs containing SDP payloads, ICE candidates, and session tokens are automatically redacted to ensure credential safety.

---

## Getting Started

### Prerequisites
- [Rust](https://www.rust-lang.org/) (edition 2024 or later)
- Cargo

### Installation & Run

1. Clone or navigate to the directory:
   ```bash
   cd backend
   ```

2. Copy environmental variables:
   ```bash
   cp .env.example .env
   ```

3. Update `.env` with your TURN server credentials and allowed origins.

4. Run unit and integration tests:
   ```bash
   cargo test
   ```

5. Start the signaling server:
   ```bash
   cargo run --release
   ```

---

## API Specifications

### HTTP API
- **GET `/api/config`**: Fetches the WebRTC TURN configuration for the frontend client. Returns JSON:
  ```json
  {
    "turnUrl": "turn:your-turn-server.com:3478",
    "turnUsername": "your-username",
    "turnPassword": "your-password",
    "maxMessageSize": 65536
  }
  ```

---

## WebSocket Messaging Protocol (v1)

Connection Endpoint: `ws://127.0.0.1:8000/ws`

Every message sent across the socket must include a `"version": "1"` field.

### Client-to-Server Messages

#### 1. Peer Registration
```json
{
  "version": "1",
  "type": "register",
  "role": "sender"
}
```
*Roles can be `sender` or `receiver`.*

#### 2. Create Session (Sender Only)
```json
{
  "version": "1",
  "type": "create-session"
}
```

#### 3. Join Session (Receiver Only)
```json
{
  "version": "1",
  "type": "join-session",
  "transferId": "X7K9Q2LM"
}
```

#### 4. Approve Join Request (Sender Only)
```json
{
  "version": "1",
  "type": "approve-join",
  "transferId": "X7K9Q2LM",
  "token": "secure_token_here",
  "receiverId": "394c24a3-ecc6-4f74-bb2a-332ec76a99bd"
}
```

#### 5. Offer Relay (WebRTC SDP)
```json
{
  "version": "1",
  "type": "offer",
  "transferId": "X7K9Q2LM",
  "sdp": "..."
}
```

#### 6. Answer Relay (WebRTC SDP)
```json
{
  "version": "1",
  "type": "answer",
  "transferId": "X7K9Q2LM",
  "sdp": "..."
}
```

#### 7. ICE Candidate Relay
```json
{
  "version": "1",
  "type": "ice-candidate",
  "transferId": "X7K9Q2LM",
  "candidate": "...",
  "sdpMid": "0",
  "sdpMLineIndex": 0
}
```

#### 8. Heartbeat (Ping)
```json
{
  "version": "1",
  "type": "heartbeat"
}
```

#### 9. Disconnect
```json
{
  "version": "1",
  "type": "disconnect"
}
```

---

### Server-to-Client Messages

#### 1. Registration Confirmation
```json
{
  "version": "1",
  "type": "registered",
  "peerId": "uuid"
}
```

#### 2. Session Created
```json
{
  "version": "1",
  "type": "session-created",
  "transferId": "X7K9Q2LM",
  "token": "32_char_token_value"
}
```

#### 3. Join Request Notification (To Sender)
```json
{
  "version": "1",
  "type": "join-request",
  "receiverId": "uuid"
}
```

#### 4. Session Joined Confirmation (To Receiver)
```json
{
  "version": "1",
  "type": "session-joined",
  "transferId": "X7K9Q2LM"
}
```

#### 5. Peer Joined Notification (To Sender)
```json
{
  "version": "1",
  "type": "peer-joined",
  "peerId": "uuid",
  "role": "receiver"
}
```

#### 6. Peer Disconnected Notification (To Remaining Partner)
```json
{
  "version": "1",
  "type": "peer-disconnected",
  "peerId": "uuid",
  "role": "receiver"
}
```

#### 7. Relay SDP Offer / Answer
```json
{
  "version": "1",
  "type": "relay-offer",
  "sdp": "..."
}
```

#### 8. Relay ICE Candidate
```json
{
  "version": "1",
  "type": "relay-ice-candidate",
  "candidate": "...",
  "sdpMid": "0",
  "sdpMLineIndex": 0
}
```

#### 9. Heartbeat Acknowledgment
```json
{
  "version": "1",
  "type": "heartbeat-ack"
}
```

#### 10. Error Response
```json
{
  "version": "1",
  "type": "error",
  "code": "INVALID_SESSION",
  "message": "Session does not exist"
}
```
