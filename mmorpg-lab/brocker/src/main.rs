// brocker/src/main.rs
use game_sockets::GameSocketBackend;
use bevy::prelude::*;
use std::collections::HashMap;
use uuid::Uuid;
use game_sockets::{BackendCommand, GameNetworkEvent};
use game_sockets::protocols::QuicBackend;

mod brocker_net;
// 🌟 Pull in the network logic file

#[derive(Resource, Default)]
pub struct PubSubManager {
    pub subscriptions: HashMap<[u8; 32], Vec<Uuid>>,
    pub client_connections: HashMap<u32, Uuid>,
    pub network_to_client_id: HashMap<Uuid, u32>,
    pub shard_connections: Vec<Uuid>,
}

#[derive(Resource)]
pub struct BrockerChannels {
    pub event_rx: tokio::sync::mpsc::UnboundedReceiver<GameNetworkEvent>,
    pub command_tx: tokio::sync::mpsc::UnboundedSender<BackendCommand>,
}

fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        .init_resource::<PubSubManager>()
        .add_systems(Startup, setup_brocker_network)
        // 🌟 Reference our cleanly extracted system out of broker_net
        .add_systems(Update, brocker_net::route_pubsub_traffic)
        .run();
}

fn setup_brocker_network(mut commands: Commands) {
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<GameNetworkEvent>();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<BackendCommand>();

    let bind_command = BackendCommand::Bind {
        addr: "0.0.0.0".to_string(),
        port: 5000,
    };
    let _ = command_tx.send(bind_command);

    tokio::spawn(async move {
        let backend = QuicBackend::new();
        backend.run(command_rx, event_tx);
    });

    commands.insert_resource(BrockerChannels { event_rx, command_tx });
    println!("📡 [PubSub Brocker] Engine online on port 5000. Binary layout ready.");
}