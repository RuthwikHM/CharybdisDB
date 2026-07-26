use axum::{
    Json, Router, debug_handler,
    extract::{Path, Query, State},
    http::{StatusCode, Uri},
    routing::{delete, get, put},
};

mod bloom_filter;
mod storage;
mod utils;
use crate::{storage::KVStore, utils::Range};

#[debug_handler]
async fn insert_key(
    Path(key): Path<String>,
    State(kv_store): State<KVStore>,
    body: String,
) -> StatusCode {
    println!("Put {:?} {:?} called", key, body);
    if key.is_empty() {
        return StatusCode::BAD_REQUEST;
    } else {
        kv_store.put(key, body).await;
        return StatusCode::OK;
    }
}

#[debug_handler]
async fn get_key(Path(key): Path<String>, State(kv_store): State<KVStore>) -> (StatusCode, String) {
    println!("Get {:?} called", key);
    let value = kv_store.get(&key).await;
    if value == "" {
        return (StatusCode::NOT_FOUND, "Key doesnt exist".to_string());
    } else {
        return (StatusCode::OK, value);
    }
}

#[debug_handler]
async fn delete_key(Path(key): Path<String>, State(kv_store): State<KVStore>) -> StatusCode {
    println!("Delete {:?} called", key);
    kv_store.delete(key).await;
    return StatusCode::OK;
}

#[debug_handler]
async fn scan(
    Query(range): Query<Range>,
    State(kv_store): State<KVStore>,
) -> Result<Json<Vec<String>>, StatusCode> {
    println!("Scan [{:?}, {:?}) called", range.start, range.end);
    let keys = kv_store.scan(range).await;
    if keys.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    return Ok(Json(keys));
}

#[tokio::main]
async fn main() {
    let kv_store = KVStore::new().await;
    let app = Router::new()
        .route("/:key", put(insert_key))
        .route("/keys/:key", get(get_key))
        .route("/:key", delete(delete_key))
        .route("/scan", get(scan))
        .fallback(|uri: Uri| async move {
            println!("AXUM FALLBACK HIT: {}", uri);
            StatusCode::NOT_FOUND
        })
        .with_state(kv_store);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8000")
        .await
        .unwrap();

    println!("Server started");
    axum::serve(listener, app).await.unwrap()
}
