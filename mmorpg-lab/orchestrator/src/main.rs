use game_sockets::{GameNetworkEvent, BackendCommand, GameStreamReliability};
use game_sockets::protocols::QuicBackend;
use game_sockets::GameSocketBackend; // Ensures backend execution traits are available
use redis::*;
use std::time::Duration;
use uuid::Uuid;



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_client = redis::Client::open("redis://127.0.0.1:6379")?;
    let mut redis_connection = redis_client.get_multiplexed_async_connection().await?;

    let orch_port = 8080;

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<GameNetworkEvent>();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<BackendCommand>();

    let bind_command = BackendCommand::Bind {
        addr: "0.0.0.0".to_string(),
        port: orch_port,
    };

    command_tx.send(bind_command)?;


    let backend = QuicBackend::new();

    std::thread::spawn(move || {
        backend.run(command_rx, event_tx);
    });
    println!("Orchestratorrunning on port {}", orch_port);

    // Ecoute du hearthbeat
    let mut heartbeat_redis = redis_connection.clone();
    let heartbeat_task = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {

                GameNetworkEvent::Connected(connection) => {
                    println!("Dedicated server connected with connection ID: {:?}", connection.connection_id);

                    let control_stream = game_sockets::GameStream::new(1, game_sockets::GameStreamReliability::Reliable);

                    let wake_command = BackendCommand::Send {
                        connection: connection.connection_id,
                        stream: control_stream,
                        data: bytes::Bytes::from(""), //Upgrade a faire : Envoyer un clientInfo:Ping au lieu de message vide (Plus propre)
                    };

                    let _ = command_tx.send(wake_command);
                }

                GameNetworkEvent::Message { data, .. } => {
                    if let Ok(heartbeat) = serde_json::from_slice::<shared::Heartbeat>(&data) {
                        let server_key = format!("server:{}", heartbeat.id);

                        let status = if heartbeat.player_count >= heartbeat.max_players {
                            "full"
                        } else {
                            "avaible"
                        };

                        let metadata_json = serde_json::json!({
                            "ip": heartbeat.ip,
                            "port": heartbeat.port,
                            "zone": heartbeat.zone,
                            "status": status,
                            "player_count": heartbeat.player_count
                        }).to_string();

                        // On stocke le JSON dans le champ "metadata"
                        let _: () = redis::pipe()
                            .hset(&server_key, "metadata", metadata_json)
                            .expire(&server_key, 15)
                            .query_async(&mut heartbeat_redis)
                            .await
                            .unwrap();

                        println!("Heartbeat traité : {}", heartbeat.id);
                    }
                }
                _ => {}
            }
        }
    });

    //Spawn de server si pas assez
    let mut scaler_redis = redis_connection.clone();
    let scaler_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            interval.tick().await;

            if let Ok(available_count) = count_available_servers(&mut scaler_redis).await {
                let HOT_SERVERS_MIN = 2; //Nombre de dedicated server minimum

                if available_count < HOT_SERVERS_MIN {
                    println!("Under capacity ({} available)", available_count);

                    let dynamic_server_id = Uuid::new_v4().to_string();

                    //Calcul du prochain port dispo
                    let mut highest_port = 7000;
                    if let Ok(keys) = redis::cmd("KEYS").arg("server:*").query_async::<Vec<String>>(&mut scaler_redis).await {
                        for key in keys {
                            if let Ok(metadata_str) = redis::cmd("HGET").arg(&key).arg("metadata").query_async::<String>(&mut scaler_redis).await {
                                if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&metadata_str) {
                                    if let Some(p) = json_value.get("port").and_then(|v| v.as_u64()) {
                                        let port_u16 = p as u16;
                                        if port_u16 > highest_port {
                                            highest_port = port_u16;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let next_free_port = highest_port + 1;
                    let target_zone = "zone_A";

                    // Trigger process creation with calculated inputs
                    if let Err(e) = spawn_new_dedicated_server(&dynamic_server_id, next_free_port, target_zone, orch_port) {
                        eprintln!("Error starting dedicated server : {:?}", e);
                    }/*else{
                        let server_key = format!("server:{}", dynamic_server_id);

                        let metadata_json = serde_json::json!({
                            "ip": dynamic_server_id,
                            "port": next_free_port,
                            "zone": "zone_A",
                            "status": "avaible",
                            "player_count": 1
                        }).to_string();

                        // On stocke le JSON dans le champ "metadata"
                        let _: () = redis::pipe()
                            .hset(&server_key, "metadata", metadata_json)
                            .expire(&server_key, 25)
                            .query_async(&mut scaler_redis)
                            .await
                            .unwrap();

                    }*/
                    //Decomentez si on veut ajouter instant le server dans redis (pas voulu pour le joueur puisse pas se connecter en premier avant l otchestrator
                }
            }
        }
    });

    let _ = tokio::try_join!(heartbeat_task, scaler_task);
    Ok(())
}


async fn count_available_servers(con: &mut redis::aio::MultiplexedConnection) -> Result<usize, redis::RedisError> {
    let keys: Vec<String> = redis::cmd("KEYS").arg("server:*").query_async(con).await?;
    let mut available_count = 0;

    for key in keys {
        let metadata_str: Option<String> = redis::cmd("HGET").arg(&key).arg("metadata").query_async(con).await?;

        if let Some(json_str) = metadata_str {
            // On parse dynamiquement la chaîne JSON
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str) {
                // On extrait le statut en texte de manière sécurisée
                if let Some(status) = json_value.get("status").and_then(|v| v.as_str()) {
                    if status == "avaible" {
                        available_count += 1;
                    }
                }
            }
        }
    }
    Ok(available_count)
}


fn spawn_new_dedicated_server(id: &str, port: u16, zone: &str, orch_port: u16) -> std::io::Result<()> {
    let orchestrator_addr = format!("127.0.0.1:{}", orch_port);

    let id_clone = id.to_string();
    let zone_clone = zone.to_string();

    println!("Tentative de spawn OS pour le serveur {} sur le port {}...", id, port);

    // On force l'OS à instancier le processus en dehors du pool de threads Tokio. (Conflit avec game socket)
    std::thread::spawn(move || {
        let mut command = std::process::Command::new("./target/debug/dedicated_server");

        command
            .env("DS_ID", id_clone)
            .env("DS_PORT", port.to_string())
            .env("DS_ZONE", zone_clone)
            .env("ORCH_PORT", orch_port.to_string())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit());

        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const DETACHED_PROCESS: u32 = 0x00000008;
            command.creation_flags(DETACHED_PROCESS);
        }

        match command.spawn() {
            Ok(_) => println!("Serveur dédié lancé avec succès."),
            Err(e) => eprintln!("Erreur lors du spawn du serveur dédié : {:?}", e),
        }
    });

    Ok(())
}