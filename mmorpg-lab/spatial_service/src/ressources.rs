// spatial_server/src/ressources.rs
use bevy::prelude::*;
use std::collections::HashMap;
use game_sockets::{BackendCommand, GameNetworkEvent};
use crate::spatial::QuadTree;

#[derive(Resource)]
pub struct SpatialManager {
    pub quadtree: QuadTree,
    pub last_known_shards: HashMap<u32, u32>,
}

#[derive(Resource)]
pub struct NetworkChannels {
    pub command_tx: tokio::sync::mpsc::UnboundedSender<BackendCommand>,
    pub event_rx: tokio::sync::mpsc::UnboundedReceiver<GameNetworkEvent>,
    pub broker_conn_id: Option<uuid::Uuid>,
}