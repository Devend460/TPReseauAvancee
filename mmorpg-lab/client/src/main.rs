use game_sockets::{GameSocketBackend, GameNetworkEvent, BackendCommand, GameStreamReliability};
use game_sockets::protocols::QuicBackend;
use shared::ClientInfo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = std::env::args().collect();
    let target_port: u16 = if args.len() > 1 {
        args[1].parse().unwrap_or(7001)
    } else {
        println!("No port specified in argument, defaulting to 7001");
        7001
    };

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<GameNetworkEvent>();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<BackendCommand>();

    let runtime_handle = tokio::runtime::Handle::current();
    let backend = QuicBackend::new();

    std::thread::spawn(move || {
        let _guard = runtime_handle.enter();
        backend.run(command_rx, event_tx);
    });

    println!("Connecting to server at 127.0.0.1:{}...", target_port);
    command_tx.send(BackendCommand::Connect {
        addr: "127.0.0.1".to_string(),
        port: target_port,
    })?;


    while let Some(event) = event_rx.recv().await {
        match event {
            GameNetworkEvent::Connected(connection) => {
                println!("Connected to server : {:?}", connection.connection_id);

                let create_stream_cmd = BackendCommand::CreateStream {
                    connection: connection.connection_id,
                    stream: 0,
                    reliability: GameStreamReliability::Reliable,
                };
                let _ = command_tx.send(create_stream_cmd);
                println!("Sent request for CreateStream at server");
            }

            GameNetworkEvent::StreamCreated(connection, stream) => {
                println!("💎 Stream {:?} is now fully initialized and registered!", stream);

                    //Un seul username, pas le temps pour un argument (pas la priorite, pas necessaire)
                    let join_payload = ClientInfo::Join {
                        username: "Xx_DarkSasuke_xX".to_string(),
                    };

                    if let Ok(serialized_data) = serde_json::to_vec(&join_payload) {
                        let send_command = BackendCommand::Send {
                            connection: connection.connection_id,
                            stream: stream,
                            data: bytes::Bytes::from(serialized_data),
                        };

                        let _ = command_tx.send(send_command);
                        println!("Sent Join request to dedicated server");
                    }

            }

            GameNetworkEvent::Message { data, .. } => {
                if let Ok(response) = serde_json::from_slice::<shared::DStoClient>(&data) {
                    println!("Received from Server: {:?}", response);
                }
            }

            GameNetworkEvent::Disconnected(_) => {
                println!("Disconnected from the game server.");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}