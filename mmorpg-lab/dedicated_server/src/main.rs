use std::net::SocketAddr;
use std::time::Duration;
use bevy::prelude::{Commands, Entity, IntoScheduleConfigs, Query, ResMut};
mod ressources;

use bevy::{app::App, MinimalPlugins};
use bevy::app::{Startup, Update};
use bevy::prelude::Res;
use bevy::time::common_conditions::on_timer;
use quinn::crypto::ServerConfig;
use uuid::Uuid;
use game_sockets::{GameSocketBackend, GameNetworkEvent, BackendCommand, GameStreamReliability};
use game_sockets::protocols::QuicBackend;
use shared::{ClientInfo, DStoClient};
use crate::ressources::{NetworkChannels, Player};

fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(ressources::ServerConfig::from_env())
        .add_systems(Startup, bind_socket)
        .add_systems(Update, (receive_packets, send_heartbeat.run_if(on_timer(Duration::from_secs(5)))).chain())
        .run();
}

pub fn bind_socket(mut commands: Commands, config: Res<ressources::ServerConfig>) {

    //Initialisation des Unbound
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<GameNetworkEvent>();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<BackendCommand>();

    let bind_command = BackendCommand::Bind {
        addr: "0.0.0.0".to_string(), // Bind globally to accept player traffic
        port: config.port,
    };

    if let Err(e) = command_tx.send(bind_command) {
        eprintln!("Failed to send initial bind command to QUIC backend: {:?}", e);
    } else {
        println!("Dispatched Bind request to QUIC backend for port {}", config.port);
    }

    let orch_ip = config.orchestrator_addr.ip().to_string();
    let orch_port = config.orchestrator_addr.port();

    let connect_command = BackendCommand::Connect {
        addr: orch_ip,
        port: orch_port,
    };
    let _ = command_tx.send(connect_command);


    tokio::spawn(async move {

        let backend = QuicBackend::new();

        backend.run(command_rx, event_tx);
    });
    commands.insert_resource(NetworkChannels { event_rx, command_tx,orchestrator_session: None });
}

pub fn receive_packets(mut commands: Commands, mut channels: Option<ResMut<NetworkChannels>>,player_query: Query<(Entity, &Player)>,) {
    //Reception des joueur via JOIN
    let Some(mut net) = channels else { return; };

    //On recupere tous les message recus cette frame
    while let Ok(event) = net.event_rx.try_recv() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                if net.orchestrator_session.is_none() {
                    net.orchestrator_session = Some(connection.connection_id);
                    println!("📡 Telemetry uplink verified. Assigned Uuid: {}", connection.connection_id);
                }
            }

            GameNetworkEvent::Message { connection, data, .. } => {
                if let Some(ref orch_conn) = net.orchestrator_session {
                    if connection.connection_id == *orch_conn {
                        continue;
                    }
                }
                if let Ok(payload) = serde_json::from_slice::<ClientInfo>(&data) {
                    match payload {
                        ClientInfo::Join { username } => {
                            //Ajout du player(et de sa connection)
                            commands.spawn((
                                Player {
                                    id: connection.connection_id,
                                },
                            ));

                            //Construction du JSON de Welcome
                            let response_payload = DStoClient::Welcome {
                                player_id: connection.connection_id.to_string(),
                            };

                            //Serialisation du JSON
                            if let Ok(serialized_bytes) = serde_json::to_vec(&response_payload) {

                                //On ouvre la stream pour cet envoie
                                let game_stream = game_sockets::GameStream::new(
                                    1,
                                    game_sockets::GameStreamReliability::Reliable
                                );


                                let send_command = BackendCommand::Send {
                                    connection: connection.connection_id,
                                    stream: game_stream,
                                    data: bytes::Bytes::from(serialized_bytes),
                                };

                                //Envoie du message
                                if let Err(e) = net.command_tx.send(send_command) {}
                            }

                        }

                        ClientInfo::OrchestratorStart => {

                        }
                    }
                }
            }

            GameNetworkEvent::Disconnected(connection) => {
                //Supression du player quqnd il se deconnect
                for (entity, player) in player_query.iter() {
                    if player.id == connection.connection_id {
                        commands.entity(entity).despawn();
                        break;
                    }
                }
            }
            _ => {}
        }
    }


}
pub fn send_heartbeat(
    config: Res<ressources::ServerConfig>,
    player_query: Query<&Player>,
    channels: Option<Res<NetworkChannels>>,
) {
    let Some(net) = channels else { return; };

    //Si on n'est pas connecter a un orchestrator (securite)
    let Some(orchestrator_uuid) = net.orchestrator_session else {
        return;
    };

    let player_count = player_query.iter().count();

    let heartbeat_payload = shared::Heartbeat {
        id: config.id.clone(),
        ip: "127.0.0.1".to_string(),
        port: config.port,
        zone: config.zone.clone(),
        player_count,
        max_players: config.max_players,
    };

    let hertbeat_stram = game_sockets::GameStream::new(0,GameStreamReliability::Unreliable);

    if let Ok(serialized_data) = serde_json::to_vec(&heartbeat_payload) {
        let send_command = BackendCommand::Send {
            connection: orchestrator_uuid,
            stream: hertbeat_stram,
            data: bytes::Bytes::from(serialized_data),
        };

        if let Err(e) = net.command_tx.send(send_command) {}
    }
}