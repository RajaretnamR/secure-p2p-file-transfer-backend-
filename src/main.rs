use axum::{
    routing::get,
    Router,
    extract::State,
    Json,
};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use tower_http::cors::{Any, CorsLayer};

mod config;
mod errors;
mod models;
mod security;
mod state;
mod signaling;
mod ws_handler;

#[cfg(test)]
mod tests;

use config::Config;
use state::AppState;

#[derive(serde::Serialize)]
struct TurnConfigResponse {
    #[serde(rename = "turnUrl")]
    turn_url: Option<String>,
    #[serde(rename = "turnUsername")]
    turn_username: Option<String>,
    #[serde(rename = "turnPassword")]
    turn_password: Option<String>,
    #[serde(rename = "maxMessageSize")]
    max_message_size: usize,
}

#[tokio::main]
async fn main() {
    // 1. Load centralized configuration
    let config = Config::load();

    // 2. Set up production-grade tracing subscriber
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.rust_log));
    
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stdout))
        .with(env_filter)
        .init();

    info!("Starting WebRTC signaling server in {} mode", config.environment);

    // 3. Initialize AppState
    let state = AppState::new(config.clone());

    // 4. Start background cleanup task (runs every 10 seconds)
    let state_cleanup = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let notifications = state_cleanup.cleanup_stale();
            for (target_id, msg) in notifications {
                if let Some(peer) = state_cleanup.peers.get(&target_id) {
                    let _ = peer.tx.send(msg);
                }
            }
        }
    });

    // 5. Setup CORS policies
            let cors = CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any);

    // 6. Build application Router
    let app = Router::new()
        .route("/ws", get(ws_handler::websocket_handler))
        .route("/api/config", get(get_config))
        .layer(cors)
        .with_state(state.clone());

    // 7. Bind server to socket address
    let addr = SocketAddr::new(config.host, config.port);
    info!("Server listening on http://{}", addr);

    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to bind to {}: {}", addr, e));

    axum::serve(listener, app).await.unwrap();
}

/// GET handler returning WebRTC TURN configuration for the frontend
async fn get_config(State(state): State<AppState>) -> Json<TurnConfigResponse> {
    Json(TurnConfigResponse {
        turn_url: state.config.turn_url,
        turn_username: state.config.turn_username,
        turn_password: state.config.turn_password,
        max_message_size: state.config.max_message_size,
    })
}
