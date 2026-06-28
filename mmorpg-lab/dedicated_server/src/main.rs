use std::env;
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
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use std::fs::File;
use std::sync::Arc;
use tracing::info;

fn init_dedicated_server_logging() {
    let server_port = env::var("DS_PORT").unwrap_or_else(|_| "unknown_port".to_string());

    let filter_layer = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout_layer = fmt::layer().compact().with_ansi(true);

    let log_filename = format!("shard_port_{}.log", server_port);
    let log_file = File::create(&log_filename)
        .expect("Impossible de créer le fichier de log unique du shard");

    // 1. Declare file_layer FIRST
    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_writer(Arc::new(log_file));

    // 2. Now you can safely use it here
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(stdout_layer)
        .with(file_layer) // ✅ Works now!
        .init();

    tracing::info!("Log initialisé pour le serveur dédié sur le port {}", server_port);
}


#[tokio::main]
async fn main() {
    init_dedicated_server_logging();
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




    let bind_tx = command_tx.clone();
    let bind_command = BackendCommand::Bind {
        addr: "0.0.0.0".to_string(), // Bind globally to accept player traffic
        port: config.port,
    };

    if let Err(e) = bind_tx.send(bind_command) {
        info!("Failed to send initial bind command to QUIC backend: {:?}", e);
    } else {
        info!("Dispatched Bind request to QUIC backend for port {}", config.port);
    }

    let connect_tx = command_tx.clone();
    let orch_ip = config.orchestrator_addr.ip().to_string();
    let orch_port = config.orchestrator_addr.port();

    std::thread::spawn(move || {
        // Start the backend processing loop
        let backend = QuicBackend::new();

        // Spawn a background task to wait for the socket to bind, then connect
        std::thread::spawn(move || {
            //tokio::time::sleep(Duration::from_millis(500));
            info!("⏳ Delay finished. Sending Connect command to Orchestrator at {}:{}", orch_ip, orch_port);

            let connect_command = BackendCommand::Connect {
                addr: orch_ip,
                port: orch_port,
            };
            let _ = connect_tx.send(connect_command);
        });

        // Run the main backend driver
        backend.run(command_rx, event_tx);
    });

    commands.insert_resource(NetworkChannels { event_rx, command_tx,orchestrator_session: None });
    info!("Dispatched Bind request to QUIC backend for port {}", config.port);
}

pub fn receive_packets(mut commands: Commands, mut channels: Option<ResMut<NetworkChannels>>,player_query: Query<(Entity, &Player)>,config: Res<crate::ressources::ServerConfig>) {
    //Reception des joueur via JOIN
    let Some(mut net) = channels else { return; };

    //On recupere tous les message recus cette frame
    while let Ok(event) = net.event_rx.try_recv() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                if (net.orchestrator_session.is_none()){
                    info!("👤 Orchestrator connected to shard: {:?}", connection);
                    net.orchestrator_session = Some(connection.connection_id);
                }else{
                    info!("👤 Local client connected to shard: {:?}", connection);
                }

            }

            GameNetworkEvent::Message { connection, data, stream } => {
                info!("MESSAGE RESSUS");
                if let Some(ref orch_conn) = net.orchestrator_session {
                    if connection.connection_id == *orch_conn {
                        info!("Orchestrator");
                        continue;
                    }
                }
                match serde_json::from_slice::<ClientInfo>(&data) {
                    Ok(payload) => {
                        info!("🎮 Success! Parsed ClientInfo payload successfully.");
                        match payload {
                            ClientInfo::Join { username } => {
                                info!("🎮 Join recu !");
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
                                    let send_command = BackendCommand::Send {
                                        connection: connection.connection_id,
                                        stream: stream,
                                        data: bytes::Bytes::from(serialized_bytes),
                                    };

                                    //Envoie du message
                                    if let Err(e) = net.command_tx.send(send_command) {}
                                }
                            }

                            ClientInfo::OrchestratorStart => {}
                        }
                    }
                    Err(e) => {
                        // 🔍 This will reveal if your JSON payload structure is mismatched!
                        info!("❌ DEBUG: Deserialization failed! Error: {:?}. Raw string: {:?}", e, String::from_utf8_lossy(&data));
                    }
                }
            }

            GameNetworkEvent::StreamCreated (connection, stream) => {
                info!("📡 Stream {:?} created on connection {:?}", stream, connection);

                // If we don't have our Orchestrator session saved yet, this first stream
                // tells us who the Orchestrator connection handle belongs to!
                if net.orchestrator_session.is_none() {
                    info!("✅ Orchestrator connection captured from stream events: {:?}", connection);
                    net.orchestrator_session = Some(connection.connection_id);
                }
            }

            GameNetworkEvent::Disconnected(connection) => {
                let connection_id = connection.connection_id;

                // Si la liaison déconnectée était celle de l'orchestrateur, on libère le slot
                if net.orchestrator_session == Some(connection_id) {
                    net.orchestrator_session = None;
                    info!("⚠️ Liaison avec l'Orchestrateur perdue.");
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
        info!("⏳ [Dedicated Server] send_heartbeat en attente... (Pas encore de orchestrator_session active)");
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
    let heartbeat_json = serde_json::json!({
        "id": config.id.clone(),
        "ip": "127.0.0.1".to_string(),
        "port": config.port,
        "zone": config.zone.clone(),
        "status": status,
        "player_count": current_players,
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
            info!("❌ Failed to send heartbeat command: {:?}", e);
        } else {
            info!("💓 [Dedicated Server] Heartbeat json push envoyé à l'Orchestrateur.");
        }
    }
}