use std::net::SocketAddr;
use bevy::prelude::{Commands, IntoScheduleConfigs};
mod ressources;

use bevy::{app::App, MinimalPlugins};
use bevy::app::{Startup, Update};
use bevy::prelude::Res;

use game_sockets::{GameSocketBackend, GameNetworkEvent, BackendCommand};
use game_sockets::protocols::QuicBackend;
use crate::ressources::NetworkChannels;

fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(ressources::ServerConfig::from_env())
        .add_systems(Startup, bind_socket)
        .add_systems(Update, (receive_packets, send_heartbeat).chain())
        .run();
}

pub fn bind_socket(mut commands: Commands, config: Res<ressources::ServerConfig>) {

    //Initialisation des Unbound
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<GameNetworkEvent>();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<BackendCommand>();

    let bind_command = BackendCommand::Bind {
        addr: "0.0.0.0".to_string(),
        port: config.port,
    };

    if let Err(e) = command_tx.send(bind_command) {
        eprintln!("Failed to send initial bind command to QUIC backend: {:?}", e);
    } else {
        println!("Dispatched Bind request to QUIC backend for port {}", config.port);
    }

    tokio::spawn(async move {

        let backend = QuicBackend::new();

        backend.run(command_rx, event_tx);
    });
    commands.insert_resource(NetworkChannels { event_rx, command_tx });
}

pub fn receive_packets() {

}
pub fn send_heartbeat() {

}