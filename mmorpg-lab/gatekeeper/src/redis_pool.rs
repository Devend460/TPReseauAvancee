// redis_pool.rs
use axum::http::StatusCode;
use axum::Json;
use deadpool_redis::Pool;
use uuid::Uuid;

pub async fn find_available_server(pool: &Pool) -> Result<(StatusCode, Json<serde_json::Value>), StatusCode> {

    let mut conn = pool.get().await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;


    //Recuperation de tous les serveur enregistrer
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("server:*")
        .query_async(&mut conn)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;



    for key in keys {
        let server_id = key;

        let raw_json_string: Option<String> = redis::cmd("HGET")
            .arg(server_id)
            .arg("metadata")
            .query_async(&mut conn)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // 3. Check if the server was found (handling your commented-out logic safely)
        let json_text = match raw_json_string {
            Some(text) => text,
            None => continue,
        };

        let parsed_json: serde_json::Value = serde_json::from_str(&json_text)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;


        let status = parsed_json["status"].as_str().unwrap();

        if (status == "avaible"){
            //Creation du JSON

            let ip = parsed_json["ip"].as_str().unwrap();
            let port = parsed_json["port"].as_i64().unwrap();
            let zone = parsed_json["zone"].as_str().unwrap();
            let player_id = Uuid::new_v4().to_string();

            let json_correct = serde_json::json!({"player_id":player_id,"server":{
                "ip":ip,"port":port,"zone":zone
            }});
            return Ok((StatusCode::OK,Json(json_correct)));
        }

    }


    return Ok((StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "No server available"}))));

}