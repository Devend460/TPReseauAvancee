use game_sockets::{GameSocketBackend, GameNetworkEvent, BackendCommand, GameStreamReliability};
use game_sockets::protocols::QuicBackend;
use shared::ClientInfo; // Contains your ClientInfo::Join structure
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = std::env::args().collect();
    let target_port: u16 = if args.len() > 1 {
        args[1].parse().unwrap_or(7001)
    } else {
        println!("ℹ️ No port specified, defaulting to 7001");
        7001
    };

    // 2. Initialize internal MPSC messaging channels
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<GameNetworkEvent>();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<BackendCommand>();

    // 1. Get a handle to the current Tokio runtime
    let runtime_handle = tokio::runtime::Handle::current();

    // 2. Instantiate the game_sockets backend network interface
    let backend = QuicBackend::new();

    // 3. Run the network driver on an isolated OS thread with the runtime context entered
    std::thread::spawn(move || {
        let _guard = runtime_handle.enter(); // 🌟 FIX: Enter the tokio context on this thread!
        backend.run(command_rx, event_tx);
    });

    // 4. Instruct our backend to connect to the target Dedicated Server port
    println!("🔌 Connecting to game server at 127.0.0.1:{}...", target_port);
    command_tx.send(BackendCommand::Connect {
        addr: "127.0.0.1".to_string(),
        port: target_port,
    })?;

    // 5. Run the client packet tracking and event reaction loop
    while let Some(event) = event_rx.recv().await {
        match event {
            GameNetworkEvent::Connected(connection) => {
                println!("✅ Connected to server session: {:?}", connection.connection_id);

                // 🌟 STEP 1: Instruct the library backend to explicitly open stream 0
                let create_stream_cmd = BackendCommand::CreateStream {
                    connection: connection.connection_id,
                    stream: 0, // Stream ID 0
                    reliability: GameStreamReliability::Reliable,
                };
                let _ = command_tx.send(create_stream_cmd);
                println!("📡 Sent CreateStream command for Stream 0...");
            }

            GameNetworkEvent::StreamCreated(connection, stream) => {
                println!("💎 Stream {:?} is now fully initialized and registered!", stream);

                // 🌟 STEP 2: Now that the library successfully registered it, send the Join frame
                    let join_payload = ClientInfo::Join {
                        username: "ProGamer42".to_string(),
                    };

                    if let Ok(serialized_data) = serde_json::to_vec(&join_payload) {
                        let send_command = BackendCommand::Send {
                            connection: connection.connection_id,
                            stream: stream, // Use the active, registered stream handle
                            data: bytes::Bytes::from(serialized_data),
                        };

                        let _ = command_tx.send(send_command);
                        println!("🚀 Sent 'Join' registration handshake over active stream 0!");
                    }

            }

            GameNetworkEvent::Message { data, .. } => {
                // Handle incoming game state updates or welcomes from the dedicated server here
                if let Ok(response) = serde_json::from_slice::<shared::DStoClient>(&data) {
                    println!("🎮 Received from Server: {:?}", response);
                }
            }

            GameNetworkEvent::Disconnected(_) => {
                println!("❌ Disconnected from the game server.");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}