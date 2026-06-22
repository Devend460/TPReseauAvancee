// redis_pool.rs
use axum::http::StatusCode;
use axum::Json;
use deadpool_redis::Pool;
use uuid::Uuid;

pub async fn find_available_server(pool: &Pool) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    let mut conn = pool.get().await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 1. Récupérer toutes les clés d'enregistrement des serveurs actifs
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("server:*")
        .query_async(&mut conn)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for key in keys {
        // 2. 🌟 NOUVELLE LOGIQUE : Au lieu de HGETALL, on récupère uniquement le champ "metadata"
        let metadata_str: Option<String> = redis::cmd("HGET")
            .arg(&key)
            .arg("metadata")
            .query_async(&mut conn)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Si le champ metadata est manquant pour ce serveur, on passe au suivant
        let Some(json_str) = metadata_str else {
            continue;
        };

        // 3. 🌟 On décode la chaîne JSON présente dans metadata
        if let Ok(server_json) = serde_json::from_str::<serde_json::Value>(&json_str) {

            // 4. Extraction sécurisée du statut en minuscules ("avaible")
            let status = server_json.get("status").and_then(|v| v.as_str()).unwrap_or("");

            if status == "avaible" {
                // 5. Extraction sécurisée des propriétés internes du JSON
                let ip = server_json.get("ip")
                    .and_then(|v| v.as_str())
                    .unwrap_or("127.0.0.1")
                    .to_string();

                let port = server_json.get("port")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(7001) as u16;

                let zone = server_json.get("zone")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                // Génération d'un identifiant de session unique pour le joueur qui se connecte
                let player_id = Uuid::new_v4().to_string();

                let json_correct = serde_json::json!({
                    "player_id": player_id,
                    "server": {
                        "ip": ip,
                        "port": port,
                        "zone": zone
                    }
                });

                println!("🔀 Routed new user session to server [{}] on port {}", key, port);
                return Ok((StatusCode::OK, Json(json_correct)));
            }
        }
    }

    // Retourne une erreur 503 si aucun serveur n'a le statut "avaible"
    println!("⚠️ Login requested but no active servers matched state: 'avaible'");
    Ok((StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "No server available"}))))
}