use axum::{routing::post, Router};
use deadpool_redis::{Config, Runtime, Pool};
use std::sync::Arc;

// 1. Define your wrapper state structure
pub struct AppState {
    pub redis_pool: Pool,
}

#[tokio::main]
async fn main() {
    // 2. Configure the deadpool connection manager
    let mut cfg = Config::from_url("redis://127.0.0.1:6379");
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    // 3. Wrap the state in an Arc to ensure thread safety
    let shared_state = Arc::new(AppState { redis_pool: pool });

    // 4. Inject the state globally using Axum's `.with_state()` method
    let app = Router::new()
        .route("/login", post(handlers::login_handler))
        .with_state(shared_state); // <-- Handlers can now safely extract this state

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

mod handlers;
mod redis_utils;