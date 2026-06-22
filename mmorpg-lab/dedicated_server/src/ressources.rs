use std::collections::HashMap;
use std::net::SocketAddr;
use bevy::prelude::*;
use uuid::Uuid;
use game_sockets::{BackendCommand, GameNetworkEvent};

#[derive(Resource)]
pub struct ServerConfig {
    pub id: String,
    pub port: u16,
    pub zone: String,
    pub max_players: usize,
    pub orchestrator_addr: SocketAddr,
}
impl ServerConfig {
    pub fn from_env() -> Self {
        let port_str = std::env::var("DS_PORT").unwrap_or_else(|_| "7001".to_string());
        let port = port_str.parse::<u16>().unwrap();

        let zone = std::env::var("DS_ZONE").unwrap_or_else(|_| "zone_A".to_string());

        let id = std::env::var("DS_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

        let orchestrator_addr = std::env::var("ORCH_PORT")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string())
            .parse::<std::net::SocketAddr>()
            .unwrap();

        Self {
            id,
            port,
            zone,
            max_players: 4,
            orchestrator_addr
        }
    }
}

#[derive(Resource)]
pub struct PlayerInfo {
    pub uid: String,
    pub username: String,
}
#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<SocketAddr, PlayerInfo>,
}

#[derive(Resource)]
pub struct NetworkChannels {
    pub event_rx: tokio::sync::mpsc::UnboundedReceiver<GameNetworkEvent>,
    pub command_tx: tokio::sync::mpsc::UnboundedSender<BackendCommand>,
    pub orchestrator_session: Option<uuid::Uuid>,
}

#[derive(Component)]
pub struct Player{
    pub id:Uuid
}

#[derive(Resource, Clone)]
pub struct TokioHandleResource(pub tokio::runtime::Handle);