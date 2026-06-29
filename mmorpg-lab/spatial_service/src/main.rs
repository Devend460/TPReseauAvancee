// spatial_server/src/main.rs
mod spatial;
mod ressources;

use bevy::prelude::*;
use game_sockets::{BackendCommand, GameNetworkEvent, GameStream, GameStreamReliability, GameSocketBackend};
use game_sockets::protocols::QuicBackend;
use bytes::{Buf, Bytes};
use crate::ressources::{NetworkChannels, SpatialManager};
use crate::spatial::QuadTree;

#[tokio::main]
async fn main() {
    let mut world_tree = QuadTree::new(Rect::new(-1000.0, -1000.0, 1000.0, 1000.0), 0, 1, None);
    // Exemple : Assigne statiquement Shard 0, 1, 2, 3 aux 4 quadrants du monde
    world_tree.subdivide_statically([0, 1, 2, 3]);

    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(SpatialManager {
            quadtree: world_tree,
            last_known_shards: std::collections::HashMap::new(),
        })
        .add_systems(Startup, connect_to_broker)
        .add_systems(Update, listen_position_updates.run_if(resource_exists::<NetworkChannels>))
        .run();
}

fn connect_to_broker(mut commands: Commands) {
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<GameNetworkEvent>();
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel::<BackendCommand>();

    let backend_tx = command_tx.clone();
    std::thread::spawn(move || {
        let backend = QuicBackend::new();
        // Connexion au port du Broker (port 5000 d'après ton main.rs du Broker)
        let _ = backend_tx.send(BackendCommand::Connect { addr: "127.0.0.1".to_string(), port: 5000 });
        backend.run(command_rx, event_tx);
    });

    commands.insert_resource(NetworkChannels { command_tx, event_rx, broker_conn_id: None });
}

fn listen_position_updates(
    mut net_channels: ResMut<NetworkChannels>,
    mut spatial_manager: ResMut<SpatialManager>,
) {
    while let Ok(event) = net_channels.event_rx.try_recv() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                println!("Connecté au Broker PubSub");
                net_channels.broker_conn_id = Some(connection.connection_id);
            }
            GameNetworkEvent::Message { data, .. } => {
                if data.is_empty() { continue; }
                let mut buf = std::io::Cursor::new(&data);
                let tag = buf.get_u8();


                if tag == 0x10 {
                    if buf.remaining() >= 12 {
                        let client_id = buf.get_u32_le();
                        let x = buf.get_f32_le();
                        let y = buf.get_f32_le();
                        let pos = Vec2::new(x, y);

                        let Some(broker_id) = net_channels.broker_conn_id else { continue; };

                        //Calculer le nouveau shard_id via shard_for(pos)
                        if let Some(new_shard_id) = spatial_manager.quadtree.shard_for(pos) {

                            let old_shard_id = spatial_manager.last_known_shards.get(&client_id).cloned();

                            //Si le shard a changé -> Unsubscribe(ancien) puis Subscribe(nouveau)
                            if old_shard_id != Some(new_shard_id) {
                                let stream = GameStream::new(0, GameStreamReliability::Reliable);

                                //Envoyer Unsubscribe pour l'ancien shard (si existant)
                                if let Some(old_id) = old_shard_id {
                                    let mut unsub_bytes = Vec::with_capacity(37);
                                    unsub_bytes.push(0x02); // Tag Unsubscribe
                                    unsub_bytes.extend_from_slice(&client_id.to_le_bytes());
                                    unsub_bytes.extend_from_slice(&generate_shard_topic(old_id));

                                    let _ = net_channels.command_tx.send(BackendCommand::Send {
                                        connection: broker_id,
                                        stream: stream.clone(),
                                        data: Bytes::from(unsub_bytes),
                                    });
                                }

                                //Envoyer Subscribe pour le nouveau shard
                                let mut sub_bytes = Vec::with_capacity(37);
                                sub_bytes.push(0x01); // Tag Subscribe
                                sub_bytes.extend_from_slice(&client_id.to_le_bytes());
                                sub_bytes.extend_from_slice(&generate_shard_topic(new_shard_id));

                                let _ = net_channels.command_tx.send(BackendCommand::Send {
                                    connection: broker_id,
                                    stream: stream.clone(),
                                    data: Bytes::from(sub_bytes),
                                });

                                // Mettre à jour l'historique
                                spatial_manager.last_known_shards.insert(client_id, new_shard_id);
                                println!("Client {} a migré vers le shard:{}", client_id, new_shard_id);
                            }
                        }

                        //Si shards_near(pos, margin) retourne plusieurs shards alors on lance la CrossingAlert
                        let margin = 50.0;
                        let near_shards = spatial_manager.quadtree.shards_near(pos, margin);

                        if near_shards.len() > 1 {
                            if let Some(&current_shard) = spatial_manager.last_known_shards.get(&client_id) {
                                // Trouver le shard voisin vers lequel le joueur se dirige
                                if let Some(&target_shard) = near_shards.iter().find(|&&id| id != current_shard) {

                                    // Format du paquet conforme : [ Tag (0x15) | Topic Shard Cible (32 octets) | Client ID (4 octets) ]
                                    let mut alert_packet = Vec::with_capacity(1 + 32 + 4);

                                    alert_packet.push(0x15);

                                    let target_topic = generate_shard_topic(target_shard);
                                    alert_packet.extend_from_slice(&target_topic);

                                    alert_packet.extend_from_slice(&client_id.to_le_bytes());

                                    // Envoi au Broker du Crossing Alert
                                    let stream = GameStream::new(0, GameStreamReliability::Reliable);
                                    let _ = net_channels.command_tx.send(BackendCommand::Send {
                                        connection: broker_id,
                                        stream,
                                        data: bytes::Bytes::from(alert_packet),
                                    });

                                    println!("⚠️ [CrossingAlert] Signal 0x15 envoyé pour le joueur {} (Cible: Shard {})", client_id, target_shard);
                                }
                            }
                        }
                        }
                    }
                }

            _ => {}
        }
    }
}

// Helper pour générer un topic au format [u8; 32] comme attendu par le Broker
fn generate_shard_topic(shard_id: u32) -> [u8; 32] {
    let topic_str = format!("shard:{}", shard_id);
    let mut topic_bytes = [0u8; 32];
    let s_bytes = topic_str.as_bytes();
    let len = s_bytes.len().min(32);
    topic_bytes[..len].copy_from_slice(&s_bytes[..len]);
    topic_bytes
}