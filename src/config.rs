use std::env;
use std::net::IpAddr;
use tracing::info;

#[derive(Debug, Clone)]
pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    pub environment: String,
    pub rust_log: String,
    pub session_timeout_minutes: u64,
    pub heartbeat_timeout_seconds: u64,
    pub max_message_size: usize,
    pub max_connections: usize,
    pub allowed_origins: Vec<String>,
    pub turn_url: Option<String>,
    pub turn_username: Option<String>,
    pub turn_password: Option<String>,
}

impl Config {
    pub fn load() -> Self {
        if let Err(e) = dotenvy::dotenv() {
            info!(
                "No .env file found or error loading it: {}. Using environment variables.",
                e
            );
        }

        let host_str = env::var("HOST")
            .unwrap_or_else(|_| "0.0.0.0".to_string());

        let host: IpAddr = host_str
            .parse()
            .unwrap_or_else(|_| panic!("Invalid HOST address: {}", host_str));

        let port: u16 = env::var("PORT")
            .unwrap_or_else(|_| "8000".to_string())
            .parse()
            .expect("PORT must be valid");

        let environment = env::var("ENVIRONMENT")
            .unwrap_or_else(|_| "development".to_string());

        if environment != "development" && environment != "production" {
            panic!("ENVIRONMENT must be either development or production");
        }

        let rust_log = env::var("RUST_LOG")
            .unwrap_or_else(|_| "info,backend=debug".to_string());

        let session_timeout_minutes: u64 = env::var("SESSION_TIMEOUT_MINUTES")
            .unwrap_or_else(|_| "15".to_string())
            .parse()
            .expect("SESSION_TIMEOUT_MINUTES must be valid");

        // FIXED FROM 60 -> 180
        let heartbeat_timeout_seconds: u64 = env::var("HEARTBEAT_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "180".to_string())
            .parse()
            .expect("HEARTBEAT_TIMEOUT_SECONDS must be valid");

        let max_message_size: usize = env::var("MAX_MESSAGE_SIZE")
            .unwrap_or_else(|_| "65536".to_string())
            .parse()
            .expect("MAX_MESSAGE_SIZE must be valid");

        let max_connections: usize = env::var("MAX_CONNECTIONS")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .expect("MAX_CONNECTIONS must be valid");

        let allowed_origins_str = env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "*".to_string());

        let allowed_origins = allowed_origins_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let turn_url = env::var("TURN_URL")
            .ok()
            .filter(|s| !s.trim().is_empty());

        let turn_username = env::var("TURN_USERNAME")
            .ok()
            .filter(|s| !s.trim().is_empty());

        let turn_password = env::var("TURN_PASSWORD")
            .ok()
            .filter(|s| !s.trim().is_empty());

        Config {
            host,
            port,
            environment,
            rust_log,
            session_timeout_minutes,
            heartbeat_timeout_seconds,
            max_message_size,
            max_connections,
            allowed_origins,
            turn_url,
            turn_username,
            turn_password,
        }
    }
}