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
                    // Map bytes directly to shared packet
                    if let Ok(heartbeat) = serde_json::from_slice::<shared::Heartbeat>(&data) {
                        let server_key = format!("server:{}", heartbeat.id);

                        // Compute current flag state
                        let status = if heartbeat.player_count >= heartbeat.max_players {
                            "full"
                        } else {
                            "AVAIBLE"
                        };

                        // Write data fields straight to Redis
                        let _: () = redis::pipe()
                            .hset(&server_key, "ip", &heartbeat.ip)
                            .hset(&server_key, "port", heartbeat.port)
                            .hset(&server_key, "zone", &heartbeat.zone)
                            .hset(&server_key, "status", status)
                            .hset(&server_key, "players", heartbeat.player_count)
                            .expire(&server_key, 15) // Prunes automatically if heartbeats fail
                            .query_async(&mut heartbeat_redis)
                            .await
                            .unwrap();
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
                            // 🌟 FIXED: Changed `<_, String>` to `<String>`
                            if let Ok(p_str) = redis::cmd("HGET").arg(&key).arg("port").query_async::<String>(&mut scaler_redis).await {
                                if let Ok(p) = p_str.parse::<u16>() {
                                    if p > highest_port {
                                        highest_port = p;
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
        let status: Option<String> = redis::cmd("HGET").arg(&key).arg("status").query_async(con).await?;
        if let Some(s) = status {
            if s == "AVAIBLE" {
                available_count += 1;
            }
        }
    }
    Ok(available_count)
}

// OS system invocation wrapper
fn spawn_new_dedicated_server(id: &str, port: u16, zone: &str, orch_port: u16) -> std::io::Result<()> {
    // Formulate the full SocketAddr format that ressources.rs expects
    let orchestrator_addr = format!("127.0.0.1:{}", orch_port);

    std::process::Command::new("./target/release/dedicated_server")
        // Inject the precise configuration fields
        .env("DS_ID", id)                  // Used inside loop 🚀
        .env("DS_PORT", port.to_string())  // Calculated inside loop 🚀
        .env("DS_ZONE", zone)              // Defined inside loop 🚀
        .env("ORCH_PORT", orchestrator_addr)
        .spawn()?; // Detach into background daemon processing

    println!("✨ Process spawned -> Dedicated Server [ID: {}, Port: {}, Zone: {}]", id, port, zone);
    Ok(())
}