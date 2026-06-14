use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Json, IntoResponse},
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetEvent {
    pub event_type: String,
    pub source: String,
    pub payload: String,
    pub ternary_merit: i8, // -1, 0, +1
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub topic: String,
    pub callback_url: Option<String>,
}

#[derive(Clone)]
struct AppState {
    events: Arc<RwLock<Vec<FleetEvent>>>,
    subscriptions: Arc<RwLock<HashMap<String, Vec<Subscription>>>>,
}

async fn publish_event(
    State(state): State<AppState>,
    Json(event): Json<FleetEvent>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut events = state.events.write().await;
    events.push(event.clone());
    // Keep last 10000
    let overflow = events.len().saturating_sub(10000);
    if overflow > 0 {
        let drain_end = overflow.min(events.len());
        events.drain(0..drain_end);
    }

    // Check subscriptions
    let subs = state.subscriptions.read().await;
    let matched = subs.get(&event.event_type).map(|s| s.len()).unwrap_or(0);

    tracing::info!(
        event_type = %event.event_type,
        source = %event.source,
        merit = event.ternary_merit,
        subscribers = matched,
        "event published"
    );

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "accepted": true,
            "event_type": event.event_type,
            "subscribers": matched,
        })),
    )
}

async fn subscribe(
    State(state): State<AppState>,
    Json(sub): Json<Subscription>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut subs = state.subscriptions.write().await;
    subs.entry(sub.topic.clone())
        .or_default()
        .push(sub.clone());

    (
        StatusCode::CREATED,
        Json(serde_json::json!({"subscribed": true, "topic": sub.topic})),
    )
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    event_type: Option<String>,
    source: Option<String>,
    since: Option<String>,
    limit: Option<usize>,
}

async fn get_events(
    State(state): State<AppState>,
    Query(query): Query<EventQuery>,
) -> Json<Vec<FleetEvent>> {
    let events = state.events.read().await;
    let mut results: Vec<FleetEvent> = events.clone();
    results.reverse();

    if let Some(ref et) = query.event_type {
        results.retain(|e| e.event_type == *et);
    }
    if let Some(ref src) = query.source {
        results.retain(|e| e.source == *src);
    }
    if let Some(ref since) = query.since {
        if let Ok(dt) = since.parse::<DateTime<Utc>>() {
            results.retain(|e| e.timestamp > dt);
        }
    }

    let limit = query.limit.unwrap_or(100).min(1000);
    results.truncate(limit);
    Json(results)
}

#[derive(Debug, Serialize)]
struct StatsResponse {
    total_events: usize,
    topics: Vec<String>,
    subscriptions: usize,
}

async fn get_stats(State(state): State<AppState>) -> Json<StatsResponse> {
    let events = state.events.read().await;
    let subs = state.subscriptions.read().await;

    let mut topics: Vec<String> = events.iter()
        .map(|e| e.event_type.clone())
        .collect();
    topics.sort();
    topics.dedup();

    let sub_count: usize = subs.values().map(|v| v.len()).sum();

    Json(StatsResponse {
        total_events: events.len(),
        topics,
        subscriptions: sub_count,
    })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8782".into())
        .parse()
        .expect("PORT must be a number");

    let state = AppState {
        events: Arc::new(RwLock::new(Vec::with_capacity(10000))),
        subscriptions: Arc::new(RwLock::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/api/events", post(publish_event))
        .route("/api/events", get(get_events))
        .route("/api/subscribe", post(subscribe))
        .route("/api/stats", get(get_stats))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    println!("event-bus listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
