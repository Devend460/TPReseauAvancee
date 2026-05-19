use axum::{extract::State, Json, http::StatusCode};
use std::sync::Arc;
use serde::Deserialize;
use crate::AppState;

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}
pub async fn login_handler(State(state): State<Arc<AppState>>, Json(payload): Json<LoginRequest>) -> Json<serde_json::Value> {


    //401 Credential pas valide
    let username = payload.username.clone();
    let password = payload.password.clone();
    if (username.is_empty() || password == "1234"){
        return Json(serde_json::json!({"error":"No server avaible"}));
    }


    //503 Pas de serveur trouver

    //200 tout vas bien

    return Json(serde_json::json!({})) //temp le temps de faire les if pour eviter l'erreur
}