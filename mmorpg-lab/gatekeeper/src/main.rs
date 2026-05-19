use axum::{routing::get, Router, routing::post};
use deadpool_redis::{Config, Runtime, Pool};
use std::sync::Arc;

pub struct AppState {
    redis_pool: Pool,
}

#[tokio::main]
async fn main() {

    let mut redis_config = Config::from_url("redis://127.0.0.1:6379");
    let redis_pool = redis_config.create_pool(Some(Runtime::Tokio1)).unwrap();

    let shared_state = Arc::new(AppState {redis_pool});

    let app : Router = Router::new()
        .route("/login", post(handlers::login_handler))
        .with_state(shared_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();

}


mod handlers;