// brocker/src/broker_net.rs
use bevy::prelude::*;
use uuid::Uuid;
use game_sockets::{GameNetworkEvent, BackendCommand, GameStreamReliability, GameStream};
use bytes::{BytesMut, Buf, BufMut};
use crate::{PubSubManager, BrockerChannels};

pub fn route_pubsub_traffic(
    mut channels: Option<ResMut<BrockerChannels>>,
    mut manager: ResMut<PubSubManager>,
) {
    let Some(mut net) = channels else { return; };

    while let Ok(event) = net.event_rx.try_recv() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                println!("Liaison brute réseau établie: {}", connection.connection_id);
            }

            GameNetworkEvent::Message { connection, data, .. } => {
                if data.is_empty() { continue; }

                let mut buf = std::io::Cursor::new(&data);
                let tag = buf.get_u8();

                match tag {
                    // -----------------------------------------------------------------
                    // 0x01: SUBSCRIBE — Déclenché par le Service Spatial
                    // -----------------------------------------------------------------
                    shared::TAG_SUBSCRIBE => {
                        if buf.remaining() >= 36 {
                            let client_id = buf.get_u32_le();
                            let mut topic = [0u8; 32];
                            buf.copy_to_slice(&mut topic);

                            // 💡 ÉTAPE 1 : Emprunt IMMUTABLE d'abord. On récupère la valeur et on la copie.
                            // L'emprunt sur manager s'arrête immédiatement après la fermeture de la condition "if let"
                            if let Some(&client_uuid) = manager.client_connections.get(&client_id) {

                                // 💡 ÉTAPE 2 : Emprunt MUTABLE ensuite. Manager est totalement libre !
                                let subscribers = manager.subscriptions.entry(topic).or_default();
                                if !subscribers.contains(&client_uuid) {
                                    subscribers.push(client_uuid);
                                    println!("Client {} (UUID: {}) abonné au topic {:?}", client_id, client_uuid, topic);
                                }
                            } else {
                                println!("⚠Impossible d'abonner le client {}: connexion non enregistrée", client_id);
                            }
                        }
                    }

                    // -----------------------------------------------------------------
                    // 0x02: UNSUBSCRIBE — Déclenché par le Service Spatial
                    // -----------------------------------------------------------------
                    shared::TAG_UNSUBSCRIBE => {
                        if buf.remaining() >= 36 {
                            let client_id = buf.get_u32_le();
                            let mut topic = [0u8; 32];
                            buf.copy_to_slice(&mut topic);

                            // 💡 ÉTAPE 1 : Emprunt IMMUTABLE pour lire l'UUID
                            if let Some(&client_uuid) = manager.client_connections.get(&client_id) {

                                // 💡 ÉTAPE 2 : Emprunt MUTABLE pour nettoyer la liste
                                if let Some(subscribers) = manager.subscriptions.get_mut(&topic) {
                                    subscribers.retain(|&uuid| uuid != client_uuid);
                                    println!("Client {} désabonné du topic {:?}", client_id, topic);
                                }
                            }
                        }
                    }

                    // -----------------------------------------------------------------
                    // 0x03: PUBLISH (Émis par un Shard de simulation)
                    // -----------------------------------------------------------------
                    shared::TAG_PUBLISH => {
                        if buf.remaining() >= 34 {
                            let mut topic = [0u8; 32];
                            buf.copy_to_slice(&mut topic);
                            let payload_len = buf.get_u16_le() as usize;

                            // Identification automatique du Shard
                            if !manager.shard_connections.contains(&connection.connection_id) {
                                println!("Shard identifié sur la connexion: {}", connection.connection_id);
                                manager.shard_connections.push(connection.connection_id);
                            }

                            if buf.remaining() >= payload_len {
                                let start = buf.position() as usize;
                                let raw_payload = &data[start..start + payload_len];

                                // 1. Construction du paquet de Broadcast (0x04) pour les Clients joueurs
                                let mut bcast = BytesMut::with_capacity(3 + payload_len);
                                bcast.put_u8(shared::TAG_BROADCAST);
                                bcast.put_u16_le(payload_len as u16);
                                bcast.put_slice(raw_payload);
                                let bcast_bytes = bcast.freeze();

                                let stream = GameStream::new(0, GameStreamReliability::Unreliable);

                                // Envoi à tous les clients abonnés au topic
                                if let Some(subscribers) = manager.subscriptions.get(&topic) {
                                    for &client_uuid in subscribers {
                                        let _ = net.command_tx.send(BackendCommand::Send {
                                            connection: client_uuid,
                                            stream: stream.clone(),
                                            data: bcast_bytes.clone(),
                                        });
                                    }
                                }

                                //Envoie au brocker
                                if let Some(spatial_uuid) = manager.spatial_server_connection {
                                    let _ = net.command_tx.send(BackendCommand::Send {
                                        connection: spatial_uuid,
                                        stream: stream.clone(),
                                        data: data.clone(), // On duplique le paquet original reçu du Shard
                                    });
                                }
                            }
                        }
                    }

                    // -----------------------------------------------------------------
                    // 0x05: CLIENT INPUT (Émis par le Client joueur)
                    // -----------------------------------------------------------------
                    shared::TAG_CLIENT_INPUT => {
                        if buf.remaining() >= 20 {
                            let client_id = buf.get_u32_le();

                            // Enregistrement dynamique de la session du joueur lors de son premier input
                            if !manager.client_connections.contains_key(&client_id) {
                                println!("👤 [Broker] Mapping validé : Client ID {} -> Connexion {}", client_id, connection.connection_id);
                                manager.client_connections.insert(client_id, connection.connection_id);
                                manager.network_to_client_id.insert(connection.connection_id, client_id);
                            }

                            // Relais exclusif du message vers les vrais Shards de simulation
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


                    // -----------------------------------------------------------------
                    // 0x10: Position update des hard vers le spatial service
                    // -----------------------------------------------------------------
                    shared::TAG_POSITION_UPDATE => {
                        // Relayer le PositionUpdate brute directement au Serveur Spatial
                        if let Some(spatial_uuid) = manager.spatial_server_connection {
                            let stream = GameStream::new(0, GameStreamReliability::Unreliable);
                            let _ = net.command_tx.send(BackendCommand::Send {
                                connection: spatial_uuid,
                                stream,
                                data: data.clone(),
                            });
                        }
                    }

                    // -----------------------------------------------------------------
                    // 0x15: CROSSING ALERT — Émis par le Service Spatial
                    // Format : [ TAG (0x15) | TOPIC_CIBLE ([u8; 32]) | CLIENT_ID (u32) ]
                    // -----------------------------------------------------------------
                    0x15 => {
                        if buf.remaining() >= 36 {
                            let mut target_topic = [0u8; 32];
                            buf.copy_to_slice(&mut target_topic);
                            let client_id = buf.get_u32_le();

                            // Le Broker se contente de relayer ce message (on garde le tag 0x15)
                            // à tous les Shards/entités abonnés au topic du Shard cible (ex: "shard:1")
                            if let Some(subscribers) = manager.subscriptions.get(&target_topic) {
                                let stream = GameStream::new(0, GameStreamReliability::Reliable);
                                for &subscriber_uuid in subscribers {
                                    let _ = net.command_tx.send(BackendCommand::Send {
                                        connection: subscriber_uuid,
                                        stream: stream.clone(),
                                        data: data.clone(), // On renvoie le paquet [0x15 | topic | client_id]
                                    });
                                }
                            }
                            println!("Alerte de frontière relayée pour le client {} vers le topic {:?}", client_id, target_topic);
                        }
                    }

                    _ => {
                        eprintln!("Tag binaire inconnu ou mal formé : {:#04X}", tag);
                    }
                }
            }

            GameNetworkEvent::Disconnected(connection) => {
                println!("Déconnexion détectée : {}", connection.connection_id);

                // Nettoyage si c'était un client
                if let Some(client_id) = manager.network_to_client_id.remove(&connection.connection_id) {
                    manager.client_connections.remove(&client_id);
                    for subscribers in manager.subscriptions.values_mut() {
                        subscribers.retain(|&uuid| uuid != connection.connection_id);
                    }
                    println!("Données nettoyées pour le Client joueur: {}", client_id);
                } else {
                    // Nettoyage si c'était un Shard
                    manager.shard_connections.retain(|&uuid| uuid != connection.connection_id);
                    println!("Shard retiré de la liste d'infrastructure.");
                }
            }
            _ => {}
        }
    }
}