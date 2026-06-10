// redis_pool.rs
use axum::http::StatusCode;
use axum::Json;
use deadpool_redis::Pool;
use uuid::Uuid;
use std::collections::HashMap;

pub async fn find_available_server(pool: &Pool) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
    let mut conn = pool.get().await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // 1. Fetch all active server registration keys
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("server:*")
        .query_async(&mut conn)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for key in keys {
        // 2. Fetch the entire flattened hash collection for this server entry
        let server_data: HashMap<String, String> = redis::cmd("HGETALL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // If the hash entry is empty or missing vital metadata, proceed to next
        if server_data.is_empty() {
            continue;
        }

        // 3. Extract the server status safely, matching your Orchestrator's exact string "AVAIBLE"
        let status = server_data.get("status").map(|s| s.as_str()).unwrap_or("");

        if status == "AVAIBLE" {
            // 4. Safely pull out and format properties
            let ip = server_data.get("ip").cloned().unwrap_or_else(|| "127.0.0.1".to_string());

            // Safely parse the port string back into an integer
            let port = server_data.get("port")
                .and_then(|p| p.parse::<u16>().ok())
                .unwrap_or(7001);

            let zone = server_data.get("zone").cloned().unwrap_or_else(|| "unknown".to_string());

            // Generate a fresh unique session identifier for the incoming player
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

    // Explicitly return a 503 error response if no instances matched the required status
    println!("⚠️ Login requested but no active servers matched state: 'AVAIBLE'");
    Ok((StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "No server available"}))))
}