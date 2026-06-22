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
use crate::ressources::{NetworkChannels, Player,TokioHandleResource};

#[tokio::main]
async fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(ressources::ServerConfig::from_env())
        .add_systems(Startup, bind_socket)
        .add_systems(Update, (receive_packets, send_heartbeat.run_if(on_timer(Duration::from_secs(5)))).chain()
            .run_if(bevy::prelude::resource_exists::<NetworkChannels>))
        .run();
}

pub fn bind_socket(mut commands: Commands, config: Res<ressources::ServerConfig>,) {

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
    println!("Dispatched Bind request to QUIC backend for port {}", config.port);
}

pub fn receive_packets(mut commands: Commands, mut channels: Option<ResMut<NetworkChannels>>,player_query: Query<(Entity, &Player)>,config: Res<crate::ressources::ServerConfig>) {
    //Reception des joueur via JOIN
    let Some(mut net) = channels else { return; };

    //On recupere tous les message recus cette frame
    while let Ok(event) = net.event_rx.try_recv() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                let connection_id = connection.connection_id;

                // La toute première connexion au démarrage est obligatoirement l'Orchestrateur
                if net.orchestrator_session.is_none() {
                    net.orchestrator_session = Some(connection_id);
                    println!("🔗 [Dedicated Server] Réseau QUIC Établi ! Connecté à l'Orchestrator. Session: {}", connection_id);                } else {
                    // Si l'orchestrateur est déjà lié, alors cette nouvelle connexion est un Joueur !
                    println!("🎮 [Dedicated Server] Un joueur s'est connecté ! ID: {}", connection_id);
                    commands.spawn(Player { id: connection_id });
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
                let connection_id = connection.connection_id;

                // Si la liaison déconnectée était celle de l'orchestrateur, on libère le slot
                if net.orchestrator_session == Some(connection_id) {
                    net.orchestrator_session = None;
                    println!("⚠️ Liaison avec l'Orchestrateur perdue.");
                }

                // Suppression du player quand il se déconnecte
                for (entity, player) in player_query.iter() {
                    if player.id == connection_id {
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

    // Si on n'est pas connecté à un orchestrateur, on attend
    let Some(orchestrator_uuid) = net.orchestrator_session else {
        println!("⏳ [Dedicated Server] send_heartbeat en attente... (Pas encore de orchestrator_session active)");
        return;
    };

    let current_players = player_query.iter().count();

    // 🌟 L'ALINEAMENT AVEC L'ORCHESTRATOR :
    // On détermine le statut en minuscules comme l'attend l'Orchestrator ("avaible" ou "full")
    let status = if current_players >= config.max_players {
        "full"
    } else {
        "avaible"
    };

    // On construit un objet JSON dynamique temporaire qui mappe au pixel près
    // le format de l'Orchestrator ("players_count" avec un 's')
    let heartbeat_json = serde_json::json!({
        "id": config.id.clone(),
        "ip": "127.0.0.1".to_string(), // Mettre l'IP publique ou locale de la machine du DS
        "port": config.port,
        "zone": config.zone.clone(),
        "status": status,
        "players_count": current_players, // Aligné avec "players_count" de l'Orchestrator !
        "max_players": config.max_players
    });

    let heartbeat_stream = game_sockets::GameStream::new(0, GameStreamReliability::Unreliable);

    // Sérialisation du JSON en tableau d'octets (Bytes)
    if let Ok(serialized_data) = serde_json::to_vec(&heartbeat_json) {
        let send_command = BackendCommand::Send {
            connection: orchestrator_uuid,
            stream: heartbeat_stream,
            data: bytes::Bytes::from(serialized_data),
        };

        if let Err(e) = net.command_tx.send(send_command) {
            eprintln!("❌ Failed to send heartbeat command: {:?}", e);
        } else {
            println!("💓 [Dedicated Server] Heartbeat json push envoyé à l'Orchestrateur.");
        }
    }
}