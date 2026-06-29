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
    pub orch_addr: SocketAddr,

}
impl ServerConfig {
    pub fn from_env() -> Self {
        let port_str = std::env::var("DS_PORT").unwrap_or_else(|_| "7001".to_string());
        let port = port_str.parse::<u16>().unwrap();

        let zone = std::env::var("DS_ZONE").unwrap_or_else(|_| "zone_A".to_string());

        let id = std::env::var("DS_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

        let orchestrator_port_str = std::env::var("ORCH_PORT").unwrap_or_else(|_| "8080".to_string());

        let orch_addr = format!("127.0.0.1:{}", orchestrator_port_str)
            .parse::<std::net::SocketAddr>()
            .expect("Failed to parse reconstructed Orchestrator address string");

        Self {
            id,
            port,
            zone,
            max_players: 4,
            orch_addr
        }
    }
}

#[derive(Resource)]
pub struct PlayerInfo {
    pub uid: String,
    pub username: String,
    pub entity: Entity,
}
#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<Uuid, PlayerInfo>,
}

#[derive(Resource)]
pub struct NetworkChannels {
    pub event_rx: tokio::sync::mpsc::UnboundedReceiver<GameNetworkEvent>,
    pub command_tx: tokio::sync::mpsc::UnboundedSender<BackendCommand>,
    pub orch_addr: Option<uuid::Uuid>,
}

#[derive(Component)]
pub struct Player{
    pub id:Uuid
}