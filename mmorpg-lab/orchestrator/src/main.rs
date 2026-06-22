use game_sockets::{GameNetworkEvent, BackendCommand, GameStreamReliability};
use game_sockets::protocols::QuicBackend;
use game_sockets::GameSocketBackend; // Ensures backend execution traits are available
use redis::*;
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Establish your single multiplexed connection pipeline to Redis
    let redis_client = redis::Client::open("redis://127.0.0.1:6379")?;
    let mut redis_connection = redis_client.get_multiplexed_async_connection().await?;

    // 2. Fetch the orchestrator configuration variables
    let orch_port = std::env::var("ORCH_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()?;

    // 3. MATCHING DEDICATED SERVER: Spin up your asynchronous MPSC Channels
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<GameNetworkEvent>();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<BackendCommand>();

    // Send the exact same layout Bind request
    let bind_command = BackendCommand::Bind {
        addr: "0.0.0.0".to_string(), // Listen on all network interfaces
        port: orch_port,
    };

    command_tx.send(bind_command)?;

    // Run the exact same QuicBackend pipeline thread loop
    tokio::spawn(async move {
        let backend = QuicBackend::new();
        backend.run(command_rx, event_tx);
    });
    println!("🚀 Orchestrator QUIC backend running on port {}", orch_port);

    // 4. SPAWN TASK 1: Heartbeat Event Listener
    let mut heartbeat_redis = redis_connection.clone();
    tokio::spawn(async move {
        println!("📡 Awaiting telemetry streams from active dedicated servers...");

        // Continuous receive packet loop
        while let Some(event) = event_rx.recv().await {
            match event {
                GameNetworkEvent::Message { data, .. } => {
                    if let Ok(heartbeat) = serde_json::from_slice::<shared::Heartbeat>(&data) {
                        let server_key = format!("server:{}", heartbeat.id);

                        let status = if heartbeat.player_count >= heartbeat.max_players {
                            "full"
                        } else {
                            "avaible" // 🌟 En minuscules pour correspondre à votre format !
                        };

                        // 🌟 CORRECTION 1 : On recrée exactement la structure JSON attendue
                        let metadata_json = serde_json::json!({
                            "ip": heartbeat.ip,
                            "port": heartbeat.port,
                            "zone": heartbeat.zone,
                            "status": status,
                            "players_count": heartbeat.player_count // 🌟 Renommé en players_count
                        }).to_string();

                        // On stocke le JSON dans le champ unique "metadata"
                        let _: () = redis::pipe()
                            .hset(&server_key, "metadata", metadata_json)
                            .expire(&server_key, 15)
                            .query_async(&mut heartbeat_redis)
                            .await
                            .unwrap();

                        println!("💓 Heartbeat traité et stocké dans metadata pour le serveur : {}", heartbeat.id);
                    }
                }
                _ => {}
            }
        }
    });

    // 5. TASK 2: Scaler Evaluation Loop
    let mut scaler_redis = redis_connection.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            interval.tick().await;

            if let Ok(available_count) = count_available_servers(&mut scaler_redis).await {
                let HOT_SERVERS_MIN = 2;

                if available_count < HOT_SERVERS_MIN {
                    println!("⚠️ Under capacity ({} available). Calculating next node metrics...", available_count);

                    // 🆔 CALCULATE THE ID: Fresh unique identity per loop execution
                    let dynamic_server_id = Uuid::new_v4().to_string();

                    // 🧮 CALCULATE THE PORT: Check Redis to avoid collisions
                    let mut highest_port = 7000;
                    if let Ok(keys) = redis::cmd("KEYS").arg("server:*").query_async::<Vec<String>>(&mut scaler_redis).await {
                        for key in keys {
                            // 🌟 CORRECTION 3 : On extrait metadata pour y lire le port de l'instance
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
                        eprintln!("❌ Error launching server instance process: {:?}", e);
                    }
                }
            }
        }
    });

    // Keep the main thread alive so background spawned tasks continue to execute
    std::future::pending::<()>().await;
    Ok(())
}

// Helper query function to trace database records
async fn count_available_servers(con: &mut redis::aio::MultiplexedConnection) -> Result<usize, redis::RedisError> {
    let keys: Vec<String> = redis::cmd("KEYS").arg("server:*").query_async(con).await?;
    let mut available_count = 0;

    for key in keys {
        // 🌟 CORRECTION 2 : On extrait le champ "metadata" à la place de "status"
        let metadata_str: Option<String> = redis::cmd("HGET").arg(&key).arg("metadata").query_async(con).await?;

        if let Some(json_str) = metadata_str {
            // On parse dynamiquement la chaîne JSON
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str) {
                // On extrait le statut en texte de manière sécurisée
                if let Some(status) = json_value.get("status").and_then(|v| v.as_str()) {
                    if status == "avaible" { // 🌟 Vérification du tag en minuscules
                        available_count += 1;
                    }
                }
            }
        }
    }
    Ok(available_count)
}

// OS system invocation wrapper
fn spawn_new_dedicated_server(id: &str, port: u16, zone: &str, orch_port: u16) -> std::io::Result<()> {
    let orchestrator_addr = format!("127.0.0.1:{}", orch_port);

    let id_clone = id.to_string();
    let zone_clone = zone.to_string();

    println!("🛠️ [Orchestrator] Tentative de spawn OS pour le serveur {} sur le port {}...", id, port);
    // 🌟 LA SOLUTION CÔTÉ ORCHESTRATOR :
    // On force l'OS à instancier le processus en dehors du pool de threads Tokio.
    std::thread::spawn(move || {
        let mut command = std::process::Command::new("./target/debug/dedicated_server");

        command
            .env("DS_ID", id_clone)
            .env("DS_PORT", port.to_string())
            .env("DS_ZONE", zone_clone)
            .env("ORCH_PORT", orchestrator_addr)
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit());

        // Sous Windows, on peut détacher explicitement le processus si nécessaire
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const DETACHED_PROCESS: u32 = 0x00000008;
            command.creation_flags(DETACHED_PROCESS);
        }

        match command.spawn() {
            Ok(_) => println!("✨ [Orchestrator] Serveur dédié lancé avec succès (Thread isolé)."),
            Err(e) => eprintln!("❌ [Orchestrator] Erreur lors du spawn du serveur dédié : {:?}", e),
        }
    });

    Ok(())
}