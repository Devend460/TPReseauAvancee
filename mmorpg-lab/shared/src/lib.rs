use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub status: String,
    pub player_count: usize
}

#[derive(Debug,Deserialize,Serialize,Clone)]
pub enum ClientInfo {
    #[serde(rename = "JOIN")]
    Join { username: String },

    #[serde(rename = "ORCHESTRATOR_START")]
    OrchestratorStart,
}
#[derive(Debug,Serialize,Deserialize)]
pub enum DStoClient {
    #[serde(rename = "WELCOME")]
    Welcome { player_id: String}
}

// 🌟 Network tags provided explicitly by your assignment sheets
pub const TAG_SUBSCRIBE: u8 = 0x01;
pub const TAG_UNSUBSCRIBE: u8 = 0x02;
pub const TAG_PUBLISH: u8 = 0x03;
pub const TAG_BROADCAST: u8 = 0x04;
pub const TAG_CLIENT_INPUT: u8 = 0x05;
pub const TAG_POSITION_UPDATE: u8 = 0x10;

// Inter-shard handoff protocols
pub const TAG_HANDOFF_REQUEST: u8 = 0x20;
pub const TAG_HANDOFF_ACCEPT: u8 = 0x21;
pub const TAG_HANDOFF_REJECT: u8 = 0x22;
pub const TAG_GHOST_UPDATE: u8 = 0x23;
pub const TAG_HANDOFF_COMPLETE: u8 = 0x24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityState {
    Owned,
    PendingHandoff,
    Ghost,
}

// Simple Vector helper structures for raw floating point coordinate tracking
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Vec2 {
    pub x: f32,
    pub y: f32,
}