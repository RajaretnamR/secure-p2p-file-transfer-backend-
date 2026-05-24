use rand::Rng;

/// Generates a readable, high-entropy transfer ID of 10 characters.
/// Excludes easily confused characters (like 0, O, 1, I) for better user experience.
pub fn generate_transfer_id() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut rng = rand::thread_rng();
    (0..10)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Generates a cryptographically secure token of 32 characters.
pub fn generate_secure_token() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    (0..32)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Validates WebSocket request origin against allowed origins.
pub fn is_origin_allowed(origin: &str, allowed_origins: &[String]) -> bool {
    if allowed_origins.iter().any(|o| o == "*") {
        return true;
    }
    allowed_origins.iter().any(|o| o == origin)
}

/// Redacts a sensitive token for safe logging.
pub fn redact_token(token: &str) -> String {
    if token.len() <= 6 {
        "***".to_string()
    } else {
        format!("{}...{}", &token[..3], &token[token.len() - 3..])
    }
}

/// Redacts full SDP payload for safe logging, returning only metadata size.
pub fn redact_sdp(sdp: &str) -> String {
    format!("[SDP payload ({} bytes)]", sdp.len())
}

/// Redacts ICE candidate payload details for safe logging.
pub fn redact_ice(candidate: &str) -> String {
    format!("[ICE Candidate ({} bytes)]", candidate.len())
}
