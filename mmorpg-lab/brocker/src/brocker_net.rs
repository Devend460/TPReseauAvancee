// brocker/src/broker_net.rs
use bevy::prelude::*;
use uuid::Uuid;
use game_sockets::{GameNetworkEvent, BackendCommand, GameStreamReliability, GameStream};
use bytes::{BytesMut, Buf, BufMut};
use crate::{PubSubManager, BrockerChannels};

/// System that processes incoming raw binary network streams from the QUIC backend channel
pub fn route_pubsub_traffic(
    mut channels: Option<ResMut<BrockerChannels>>,
    mut manager: ResMut<PubSubManager>,
) {
    let Some(mut net) = channels else { return; };

    while let Ok(event) = net.event_rx.try_recv() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                println!("🔌 New infrastructure link verified on Brocker: {}", connection.connection_id);
                // Dynamically cache internal loops (simulation shards or spatial microservices)
                manager.shard_connections.push(connection.connection_id);
            }

            GameNetworkEvent::Message { connection, data, .. } => {
                if data.is_empty() { continue; }

                // Turn the byte payload array into an accessible cursor
                let mut buf = std::io::Cursor::new(&data);
                let tag = buf.get_u8(); // Pop Byte 0

                match tag {
                    // -----------------------------------------------------------------
                    // 0x01: SUBSCRIBE — Triggered by Spatial Service
                    // Layout: [tag: u8] [client_id: u32] [topic: [u8; 32]]
                    // -----------------------------------------------------------------
                    shared::TAG_SUBSCRIBE => {
                        if buf.remaining() >= 36 {
                            let client_id = buf.get_u32_le();
                            let mut topic = [0u8; 32];
                            buf.copy_to_slice(&mut topic);

                            // Find the player's connection handle
                            if let Some(&client_uuid) = manager.client_connections.get(&client_id) {
                                let subscribers = manager.subscriptions.entry(topic).or_default();
                                if !subscribers.contains(&client_uuid) {
                                    subscribers.push(client_uuid);
                                    let topic_str = String::from_utf8_lossy(&topic);
                                    println!("➕ Client {} subscribed to spatial topic: {}", client_id, topic_str.trim_end_matches('\0'));
                                }
                            }
                        }
                    }

                    // -----------------------------------------------------------------
                    // 0x02: UNSUBSCRIBE — Triggered by Spatial Service
                    // Layout: [tag: u8] [client_id: u32] [topic: [u8; 32]]
                    // -----------------------------------------------------------------
                    shared::TAG_UNSUBSCRIBE => {
                        if buf.remaining() >= 36 {
                            let client_id = buf.get_u32_le();
                            let mut topic = [0u8; 32];
                            buf.copy_to_slice(&mut topic);

                            if let Some(&client_uuid) = manager.client_connections.get(&client_id) {
                                if let Some(subscribers) = manager.subscriptions.get_mut(&topic) {
                                    subscribers.retain(|&uuid| uuid != client_uuid);
                                    let topic_str = String::from_utf8_lossy(&topic);
                                    println!("➖ Client {} unsubscribed from spatial topic: {}", client_id, topic_str.trim_end_matches('\0'));
                                }
                            }
                        }
                    }

                    // -----------------------------------------------------------------
                    // 0x03: PUBLISH — Broadcast tick packets from World Shards
                    // Layout: [tag: u8] [topic: [u8; 32]] [payload_len: u16] [payload: ...]
                    // -----------------------------------------------------------------
                    shared::TAG_PUBLISH => {
                        if buf.remaining() >= 34 {
                            let mut topic = [0u8; 32];
                            buf.copy_to_slice(&mut topic);
                            let payload_len = buf.get_u16_le() as usize;

                            if buf.remaining() >= payload_len {
                                let start = buf.position() as usize;
                                let raw_payload = &data[start..start + payload_len];

                                // 🌟 Build out the unified outbound Broadcast buffer (0x04)
                                // Layout: [0x04: u8] [payload_len: u16] [payload: ...]
                                let mut bcast = BytesMut::with_capacity(3 + payload_len);
                                bcast.put_u8(shared::TAG_BROADCAST);
                                bcast.put_u16_le(payload_len as u16);
                                bcast.put_slice(raw_payload);
                                let bcast_bytes = bcast.freeze();

                                // Route snapshots out to all clients registered to this sub-quadrant topic
                                if let Some(subscribers) = manager.subscriptions.get(&topic) {
                                    let stream = GameStream::new(0, GameStreamReliability::Unreliable);
                                    for &client_uuid in subscribers {
                                        let _ = net.command_tx.send(BackendCommand::Send {
                                            connection: client_uuid,
                                            stream: stream.clone(),
                                            data: bcast_bytes.clone(),
                                        });
                                    }
                                }
                            }
                        }
                    }

                    // -----------------------------------------------------------------
                    // 0x05: CLIENT INPUT — Received from Player Clients
                    // Layout: [tag: u8] [client_id: u32] [input: [u8; 16]]
                    // -----------------------------------------------------------------
                    shared::TAG_CLIENT_INPUT => {
                        if buf.remaining() >= 20 {
                            let client_id = buf.get_u32_le();

                            // Link connection metadata on first input frame received
                            if !manager.client_connections.contains_key(&client_id) {
                                manager.client_connections.insert(client_id, connection.connection_id);
                                manager.network_to_client_id.insert(connection.connection_id, client_id);
                                println!("🎮 Registered unique routing mapping for Client ID: {}", client_id);

                                // Remove from internal node tracking array since this belongs to a player client
                                manager.shard_connections.retain(|&id| id != connection.connection_id);
                            }

                            // Relay client inputs directly to all underlying world shard solvers
                            let stream = GameStream::new(1, GameStreamReliability::Reliable);
                            for &shard_uuid in &manager.shard_connections {
                                let _ = net.command_tx.send(BackendCommand::Send {
                                    connection: shard_uuid,
                                    stream: stream.clone(),
                                    data: data.clone(),
                                });
                            }
                        }
                    }

                    _ => {
                        eprintln!("⚠️ Received unhandled byte header sequence: {:#04X}", tag);
                    }
                }
            }

            GameNetworkEvent::Disconnected(connection) => {
                println!("🔌 Connection severed from Brocker: {}", connection.connection_id);

                // Check if connection belonged to a player client
                if let Some(client_id) = manager.network_to_client_id.remove(&connection.connection_id) {
                    manager.client_connections.remove(&client_id);
                    // Wipe subscription presence from all maps
                    for subscribers in manager.subscriptions.values_mut() {
                        subscribers.retain(|&uuid| uuid != connection.connection_id);
                    }
                    println!("🧼 Garbage collector cleared session indices for Client: {}", client_id);
                } else {
                    // Wipe from infrastructure list if it was an internal server node
                    manager.shard_connections.retain(|&uuid| uuid != connection.connection_id);
                }
            }
            _ => {}
        }
    }
}