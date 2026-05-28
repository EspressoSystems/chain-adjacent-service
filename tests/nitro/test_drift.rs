use alloy::providers::Provider;
use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use chain_adjacent_service::espresso_e2e::espresso_dev_node::EspressoDevNode;

use crate::nitro_node::nitro_node::{NitroNode, NitroNodeConfig};
use crate::test_e2e::{
    CAS_FEED_URL, CasRoute, connect_l1_ws_with_retries, read_sequencer_inbox_address,
    read_tee_verifier_address, spawn_cas_with_retries, wait_for_batches_on_l1, write_cas_config,
};

const ESPRESSO_PROXY_PORT: u16 = 41100;
const ESPRESSO_UPSTREAM: &str = "http://localhost:41000";

struct EspressoProxy {
    handle: JoinHandle<()>,
    state: Arc<ProxyState>,
}

impl Drop for EspressoProxy {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

struct ProxyState {
    upstream: String,
    client: reqwest::Client,
    held: Mutex<Option<(u64, Value)>>,
    passthrough_blocks_with_txs: AtomicUsize,
    injected: AtomicBool,
    drift_blocks: usize,
}

/// Reverse-proxies every request to the upstream Espresso node, then runs
/// the response through `maybe_drift_response` so namespace block ranges can
/// be rewritten before being returned to the caller.
async fn proxy_handler(
    State(state): State<Arc<ProxyState>>,
    req: axum::extract::Request,
) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path_and_query = uri
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| uri.path().to_string());
    let url = format!("{}{path_and_query}", state.upstream);

    let headers = req.headers().clone();
    let body_bytes = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap_or_default();

    let mut upstream_req = state.client.request(method.clone(), &url);
    for (k, v) in headers.iter() {
        if k != header::HOST {
            upstream_req = upstream_req.header(k, v);
        }
    }
    if !body_bytes.is_empty() {
        upstream_req = upstream_req.body(body_bytes);
    }

    let upstream_resp = match upstream_req.send().await {
        Ok(r) => r,
        Err(err) => {
            return (
                axum::http::StatusCode::BAD_GATEWAY,
                format!("proxy error: {err}"),
            )
                .into_response();
        }
    };

    let status = upstream_resp.status();
    let resp_bytes = upstream_resp.bytes().await.unwrap_or_default().to_vec();
    let resp_bytes = maybe_drift_response(&state, &method, uri.path(), status, resp_bytes).await;

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(resp_bytes))
        .unwrap_or_else(|_| {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "build").into_response()
        })
}

/// On the first successful namespace block-range response, captures the first
/// transaction and removes it; lets `drift_blocks` worth of populated blocks
/// pass through unmodified; then re-inserts the held transaction into the next
/// block with transactions. Runs once per proxy lifetime.
async fn maybe_drift_response(
    state: &Arc<ProxyState>,
    method: &axum::http::Method,
    path: &str,
    status: axum::http::StatusCode,
    resp_bytes: Vec<u8>,
) -> Vec<u8> {
    let is_ns_range = method == axum::http::Method::GET
        && path.starts_with("/availability/block/")
        && path.contains("/namespace/");
    if !is_ns_range || !status.is_success() {
        return resp_bytes;
    }
    if state.injected.load(Ordering::SeqCst) {
        return resp_bytes;
    }
    let Some((start, _end)) = parse_block_range(path) else {
        return resp_bytes;
    };
    let Ok(mut entries) = serde_json::from_slice::<Vec<Value>>(&resp_bytes) else {
        return resp_bytes;
    };

    let mut held_guard = state.held.lock().await;
    let mut mutated = false;

    for (i, entry) in entries.iter_mut().enumerate() {
        let seq_num = start + i as u64;
        let Some(txs) = entry.get_mut("transactions").and_then(|v| v.as_array_mut()) else {
            continue;
        };
        if txs.is_empty() {
            continue;
        }

        if held_guard.is_none() {
            // Intercept the first transaction
            let captured = txs.remove(0);
            println!("[proxy] capturing first ns tx from block {seq_num}");
            *held_guard = Some((seq_num, captured));
            mutated = true;
            continue;
        }

        let passthrough = state.passthrough_blocks_with_txs.load(Ordering::SeqCst);
        if passthrough < state.drift_blocks {
            state
                .passthrough_blocks_with_txs
                .fetch_add(1, Ordering::SeqCst);
            continue;
        }

        let (origin_block, captured) = held_guard
            .take()
            .expect("held tx must be present after capture branch and beyond drift window");
        txs.insert(0, captured);
        state.injected.store(true, Ordering::SeqCst);
        println!("[proxy] re-inserted held tx (origin block {origin_block}) into block {seq_num}");
        mutated = true;
    }

    if mutated {
        serde_json::to_vec(&entries).unwrap_or(resp_bytes)
    } else {
        resp_bytes
    }
}

/// Parses `/availability/block/{start}/{end}/...` and returns `(start, end)`.
fn parse_block_range(path: &str) -> Option<(u64, u64)> {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    if parts.len() >= 5 && parts[0] == "availability" && parts[1] == "block" {
        let start = parts[2].parse().ok()?;
        let end = parts[3].parse().ok()?;
        return Some((start, end));
    }
    None
}

/// Binds the drifting reverse proxy on `ESPRESSO_PROXY_PORT` and returns a
/// handle whose `Drop` aborts the server task.
async fn start_espresso_proxy(drift_blocks: usize) -> EspressoProxy {
    let state = Arc::new(ProxyState {
        upstream: ESPRESSO_UPSTREAM.to_string(),
        client: reqwest::Client::new(),
        held: Mutex::new(None),
        passthrough_blocks_with_txs: AtomicUsize::new(0),
        injected: AtomicBool::new(false),
        drift_blocks,
    });
    let app: Router = Router::new()
        .fallback(proxy_handler)
        .with_state(state.clone());

    let addr: SocketAddr = ([127, 0, 0, 1], ESPRESSO_PROXY_PORT).into();
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind espresso proxy on {addr}: {e}"));
    println!("Espresso proxy listening on {addr}");

    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    EspressoProxy { handle, state }
}

#[tokio::test]
async fn test_e2e_message_drift() {
    let proxy = start_espresso_proxy(12).await;

    let config = NitroNodeConfig {
        no_l2_traffic: false,
    };
    let nitro_node = NitroNode::start(config).await;

    let sequencer_inbox = read_sequencer_inbox_address();
    let espresso = EspressoDevNode::connect().await;
    let starting_hotshot_height = espresso
        .client
        .fetch_latest_hotshot_block_height()
        .await
        .expect("failed to fetch latest hotshot block height")
        + 1;
    let tee_verifier_address = read_tee_verifier_address();

    let proxy_url = format!("http://localhost:{ESPRESSO_PROXY_PORT}");
    let cas_config_path = write_cas_config(
        starting_hotshot_height,
        CasRoute::Calldata,
        &sequencer_inbox,
        tee_verifier_address,
        true,
        None,
        Some(&proxy_url),
        Some(10),
    );

    let probe_url = CasRoute::Calldata.rpc_url_local();
    let cas = spawn_cas_with_retries(&cas_config_path, &probe_url).await;

    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster(CAS_FEED_URL, CasRoute::Calldata.rpc_url_for_poster());

    wait_for_batches_on_l1(&l1, from_block, 10, sequencer_inbox).await;

    let passthrough = proxy
        .state
        .passthrough_blocks_with_txs
        .load(Ordering::SeqCst);
    let injected = proxy.state.injected.load(Ordering::SeqCst);
    println!("proxy stats: ns_tx_blocks_passed={passthrough}, injected={injected}");
    assert!(injected, "held tx was never re-inserted into a later block");

    drop(cas);
    drop(nitro_node);
    drop(proxy);
}
