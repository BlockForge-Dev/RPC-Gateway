mod gateway;
mod health;
mod settings;

use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::json;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::{
    gateway::{DispatchError, Gateway},
    settings::Settings,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "rpc_gateway=info,tower_http=info,axum=info".to_string()),
        )
        .init();

    let settings = Settings::load()?;
    let bind_addr = settings.server.bind_addr.clone();
    let gateway = Arc::new(Gateway::from_settings(settings)?);
    gateway.spawn_probe_loop();
    gateway.spawn_predictive_scoring_loop();

    let app = Router::new()
        .route("/rpc", post(handle_rpc))
        .route("/health/live", get(handle_live_health))
        .route("/health/providers", get(handle_provider_health))
        .with_state(gateway);

    let listener = TcpListener::bind(bind_addr.clone()).await?;
    info!("rpc gateway listening on {}", bind_addr);

    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_rpc(State(gateway): State<Arc<Gateway>>, body: Bytes) -> impl IntoResponse {
    match gateway.execute_rpc(body).await {
        Ok(response) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );

            insert_header(&mut headers, "x-rpc-provider", &response.provider);
            insert_header(
                &mut headers,
                "x-rpc-attempts",
                &response.attempts.to_string(),
            );
            insert_header(
                &mut headers,
                "x-rpc-hedged",
                if response.hedged { "true" } else { "false" },
            );
            insert_header(
                &mut headers,
                "x-rpc-hedge-width",
                &response.hedge_width.to_string(),
            );
            insert_header(
                &mut headers,
                "x-rpc-cache",
                if response.cache_hit { "HIT" } else { "MISS" },
            );
            insert_header(
                &mut headers,
                "x-rpc-coalesced",
                if response.coalesced { "true" } else { "false" },
            );
            insert_header(
                &mut headers,
                "x-rpc-consensus-critical",
                if response.consensus_critical {
                    "true"
                } else {
                    "false"
                },
            );
            insert_header(
                &mut headers,
                "x-rpc-consensus-checked",
                if response.consensus_checked {
                    "true"
                } else {
                    "false"
                },
            );
            insert_header(
                &mut headers,
                "x-rpc-consensus-validated",
                if response.consensus_validated {
                    "true"
                } else {
                    "false"
                },
            );
            if let Some(consensus_agreement) = response.consensus_agreement.as_deref() {
                insert_header(
                    &mut headers,
                    "x-rpc-consensus-agreement",
                    consensus_agreement,
                );
            }
            if let Some(cached_at_unix_ms) = response.cached_at_unix_ms {
                insert_header(
                    &mut headers,
                    "x-rpc-cache-timestamp",
                    &cached_at_unix_ms.to_string(),
                );
            }

            (StatusCode::OK, headers, response.body).into_response()
        }
        Err(error) => {
            error!(?error, "rpc dispatch failed");
            gateway_error_response(error)
        }
    }
}

async fn handle_live_health() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

async fn handle_provider_health(State(gateway): State<Arc<Gateway>>) -> impl IntoResponse {
    let providers = gateway.provider_health();
    let adaptive_hedging = gateway.hedging_stats();
    let coalescing = gateway.coalescing_stats();
    (
        StatusCode::OK,
        Json(json!({
            "providers": providers,
            "adaptive_hedging": adaptive_hedging,
            "coalescing": coalescing,
        })),
    )
}

fn gateway_error_response(error: DispatchError) -> axum::response::Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({
            "error": "all providers failed",
            "details": error.failures,
        })),
    )
        .into_response()
}

fn insert_header(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(header_value) = HeaderValue::from_str(value) {
        headers.insert(name, header_value);
    }
}
