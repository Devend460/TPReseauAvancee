

mod ressources;

use bevy::prelude::*;
use bevy::time::common_conditions::on_timer;

use std::env;
use std::time::Duration;
use std::fs::File;
use std::sync::Arc;
use std::collections::HashMap;
use uuid::Uuid;

use game_sockets::{GameSocketBackend, GameNetworkEvent, BackendCommand, GameStreamReliability};
use game_sockets::protocols::QuicBackend;
use shared::{ClientInfo, DStoClient};
use crate::ressources::{NetworkChannels, Player};

use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use tracing::info;

/**
* Initialisation du fichier de log pour ce serveur (utilisation de marcro de tracing comme info! pour l'utiliser
*/
fn init_dedicated_server_logging() {
    let server_port = env::var("DS_PORT").unwrap_or_else(|_| "unknown_port".to_string());

    let filter_layer = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout_layer = fmt::layer().compact().with_ansi(true);

    let log_filename = format!("shard_port_{}.log", server_port);
    let log_file = File::create(&log_filename)
        .expect("Impossible de créer le fichier de log unique du shard");


    let file_layer = fmt::layer()
        .with_ansi(false)
        .with_writer(Arc::new(log_file));


    tracing_subscriber::registry()
        .with(filter_layer)
        .with(stdout_layer)
        .with(file_layer)
        .init();

    tracing::info!("Log initialisé pour le serveur dédié sur le port {}", server_port);
}


#[tokio::main]
async fn main() {
    //Initialisation du fichier de log
    init_dedicated_server_logging();
    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(ressources::ServerConfig::from_env())
        .init_resource::<crate::ressources::PlayerRegistry>()
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
        addr: "0.0.0.0".to_string(), // 0.0.0.0 On ecoute tous le monde
        port: config.port,
    };

    if let Err(e) = bind_tx.send(bind_command) {
        info!("Failed to send initial bind command to QUIC backend: {:?}", e);
    } else {
        info!("Dispatched Bind request to QUIC backend for port {}", config.port);
    }

    let connect_tx = command_tx.clone();
    let orch_ip = config.orch_addr.ip().to_string();
    let orch_port = config.orch_addr.port();

    std::thread::spawn(move || {
        // Start the backend processing loop
        let backend = QuicBackend::new();

        // Spawn a background task to wait for the socket to bind, then connect
        std::thread::spawn(move || {
            info!("Sending Connect command to Orchestrator at {}:{}", orch_ip, orch_port);

            let connect_command = BackendCommand::Connect {
                addr: orch_ip,
                port: orch_port,
            };
            let _ = connect_tx.send(connect_command);
        });

        // Run the main backend driver
        backend.run(command_rx, event_tx);
    });

    commands.insert_resource(NetworkChannels { event_rx, command_tx,orch_addr: None });
    info!("dispatched bind request to QUIC backend for port {}", config.port);
}

pub fn receive_packets(mut commands: Commands, mut channels: Option<ResMut<NetworkChannels>>,mut registry: ResMut<crate::ressources::PlayerRegistry>,config: Res<crate::ressources::ServerConfig>) {
    //Reception des joueur via JOIN
    let Some(mut net) = channels else { return; };

    //On recupere tous les message recus cette frame
    while let Ok(event) = net.event_rx.try_recv() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                //Premmier co forcement orchestrateur (L'orchestraor na pas recus de haerthbeat, donc les joueur ne peuvent pas le connaire)
                if (net.orch_addr.is_none()){
                    info!("orchestrator connected to me: {:?}", connection);
                    net.orch_addr = Some(connection.connection_id);
                }else{
                    info!("local client connected to me: {:?}", connection);
                }

            }

            GameNetworkEvent::Message { connection, data, stream } => {
                match serde_json::from_slice::<ClientInfo>(&data) {
                    Ok(payload) => {
                        match payload {
                            ClientInfo::Join { username } => {
                                info!("player as join the session, id {}",username);
                                let connection_uuid = connection.connection_id;
                                //Securite pour eviter que le meme joueur join 2 fois si il envoie plusieurs fois
                                if !registry.players.contains_key(&connection_uuid) {
                                    spawn_player(&mut commands, &mut registry, connection_uuid, username);
                                }

                                let response_payload = DStoClient::Welcome {
                                    player_id: connection.connection_id.to_string(),
                                };

                                //Serialisation du JSON
                                if let Ok(serialized_bytes) = serde_json::to_vec(&response_payload) {

                                    //On Reutilise la stream que le joueur c'est servie
                                    let send_command = BackendCommand::Send {
                                        connection: connection.connection_id,
                                        stream: stream,
                                        data: bytes::Bytes::from(serialized_bytes),
                                    };

                                    //Envoie du message
                                    if let Err(e) = net.command_tx.send(send_command) {}
                                }
                            }
                        }
                    }
                    Err(e) => {
                        info!("mysterious mesage received that i dont know ;(");
                    }
                }
            }

            GameNetworkEvent::StreamCreated (connection, stream) => {
                info!("stream {:?} created on connection {:?}", stream, connection);
            }

            GameNetworkEvent::Disconnected(connection) => {
                let connection_id = connection.connection_id;

                // Si la liaison déconnectée était celle de l'orchestrateur, on libère le slot
                if net.orch_addr == Some(connection_id) {
                    net.orch_addr = None;
                    info!("orchestrator discomected");
                }

                // Suppression du player quand il se déconnecte
                despawn_player(&mut commands, &mut registry, connection.connection_id);
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

    // Tant qu'on est pas connecter a l'orchestrateur, on attends n'execute pas
    let Some(orchestrator_uuid) = net.orch_addr else {
        info!("send_heartbeat : pas encore de orch_addr");
        return;
    };

    let current_players = player_query.iter().count();

    let heartbeat_json = serde_json::json!({
        "id": config.id.clone(),
        "ip": "127.0.0.1".to_string(),
        "port": config.port,
        "zone": config.zone.clone(),
        "player_count": current_players,
        "max_players": config.max_players
    });

    let heartbeat_stream = game_sockets::GameStream::new(0, GameStreamReliability::Unreliable);

    // Sérialisation du JSON en tableau d'octets
    if let Ok(serialized_data) = serde_json::to_vec(&heartbeat_json) {
        let send_command = BackendCommand::Send {
            connection: orchestrator_uuid,
            stream: heartbeat_stream,
            data: bytes::Bytes::from(serialized_data),
        };

        if let Err(e) = net.command_tx.send(send_command) {
            info!("Failed to send heartbeat command: {:?}", e);
        } else {
            info!("Heartbeat json sent correctly to orchestrator");
        }
    }
}


/**
 Spawn un player dans le monde en (0,0)
*/
fn spawn_player(
    commands: &mut Commands,
    registry: &mut crate::ressources::PlayerRegistry,
    connection_uuid: Uuid,
    username: String,
) {
    // Creation de l'entite player
    let player_entity = commands.spawn((
        Transform::from_xyz(0.0, 0.0, 0.0),
        GlobalTransform::default(),
    )).id();

    let info = crate::ressources::PlayerInfo {
        uid: connection_uuid.to_string(),
        username,
        entity: player_entity,
    };

    registry.players.insert(connection_uuid, info);
}

/**
 Despawn un player dans le monde en fonction de sa connection id
*/
fn despawn_player(
    commands: &mut Commands,
    registry: &mut crate::ressources::PlayerRegistry,
    connection_uuid: Uuid,
) {
    if let Some(player_info) = registry.players.remove(&connection_uuid) {
        commands.entity(player_info.entity).despawn();
    }
}