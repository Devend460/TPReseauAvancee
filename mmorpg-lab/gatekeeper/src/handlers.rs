use axum::{extract::State, Json, http::StatusCode};
use std::sync::Arc;
use serde::Deserialize;
use crate::AppState;
use crate::redis_pool;

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}
pub async fn login_handler(State(state): State<Arc<AppState>>, Json(payload): Json<LoginRequest>) -> (StatusCode, Json<serde_json::Value>) {


    //401 Credential pas valide
    let username = payload.username.clone();
    let password = payload.password.clone();
    if (username.is_empty() || password != "1234"){
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error":"Credential pas valide"})));
    }

    //Get du server

    let (status_code, json_response) = redis_pool::find_available_server(&state.redis_pool).await.unwrap_or_else(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "Redis a buger"}))
        )
    });

    return (status_code,json_response);


}
