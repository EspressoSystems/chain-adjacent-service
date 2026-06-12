use alloy::providers::Provider;
use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
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
    /// `Some` → hold the first submission for this long (out-of-order test).
    submit_delay: Option<Duration>,
    held_first_submit: AtomicBool,
    /// `> 0` → pin the reported block height for this many polls (lagging-node test).
    drift_polls: usize,
    freeze_height: AtomicU64,
    freeze_polls_remaining: AtomicUsize,
    /// Set once the proxy actually perturbs traffic, so tests can assert the scenario engaged.
    drifted: AtomicBool,
}

/// Reverse-proxies every request to the upstream Espresso node. Depending on its mode it either
/// delays the first submission (forcing a real out-of-order finalization) or pins the reported
/// block height (simulating a lagging node). Reads are otherwise passed through untouched, so the
/// light client always verifies real, consensus-finalized data.
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

    // Out-of-order injection: hold the first submission so its (earlier-sequence) tx is finalized
    // after later ones. Nothing is rewritten, so the data the light client later reads is genuine.
    if let Some(delay) = state.submit_delay {
        let is_submit = method == axum::http::Method::POST && uri.path().ends_with("/submit");
        if is_submit && !state.held_first_submit.swap(true, Ordering::SeqCst) {
            println!("[proxy] holding first submission for {delay:?}");
            state.drifted.store(true, Ordering::SeqCst);
            tokio::time::sleep(delay).await;
        }
    }

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
    let resp_headers = upstream_resp.headers().clone();
    let mut resp_bytes = upstream_resp.bytes().await.unwrap_or_default().to_vec();
    if state.drift_polls > 0 {
        resp_bytes = maybe_pin_block_height(&state, &method, uri.path(), status, resp_bytes);
    }

    // Relay the node's status and headers unchanged (Content-Type, redirect Location):
    // rewriting them breaks the client. Skip framing headers; the body is re-sent here.
    let mut builder = Response::builder().status(status);
    for (k, v) in resp_headers.iter() {
        if k != header::CONTENT_LENGTH && k != header::TRANSFER_ENCODING {
            builder = builder.header(k, v);
        }
    }
    builder
        .body(axum::body::Body::from(resp_bytes))
        .unwrap_or_else(|_| {
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "build").into_response()
        })
}

/// Simulates a lagging query node: once the node first reports a tip of at least
/// `MIN_FREEZE_HEIGHT`, pin the reported `node/block-height` at that value for `drift_polls`
/// polls, then release the true tip so the service has to catch up.
fn maybe_pin_block_height(
    state: &Arc<ProxyState>,
    method: &axum::http::Method,
    path: &str,
    status: axum::http::StatusCode,
    resp_bytes: Vec<u8>,
) -> Vec<u8> {
    let is_block_height = method == axum::http::Method::GET && path.ends_with("node/block-height");
    // The height comes back in the light client's binary form: a 4-byte header followed by the
    // height as a little-endian u64 (12 bytes total). Only rewrite that exact shape.
    if !is_block_height || !status.is_success() || resp_bytes.len() != 12 {
        return resp_bytes;
    }
    let true_height = u64::from_le_bytes(resp_bytes[4..12].try_into().expect("8 bytes"));

    // Start the window once there is some history to deliver before the stall.
    const MIN_FREEZE_HEIGHT: u64 = 4;
    if state.freeze_height.load(Ordering::SeqCst) == 0 && true_height >= MIN_FREEZE_HEIGHT {
        state.freeze_height.store(true_height, Ordering::SeqCst);
        state
            .freeze_polls_remaining
            .store(state.drift_polls, Ordering::SeqCst);
        println!(
            "[proxy] pinning block height at {true_height} for {} polls",
            state.drift_polls
        );
    }

    let frozen = state.freeze_height.load(Ordering::SeqCst);
    if frozen == 0 || state.freeze_polls_remaining.load(Ordering::SeqCst) == 0 {
        return resp_bytes;
    }

    state.freeze_polls_remaining.fetch_sub(1, Ordering::SeqCst);
    state.drifted.store(true, Ordering::SeqCst);
    let mut pinned = resp_bytes;
    pinned[4..12].copy_from_slice(&frozen.to_le_bytes());
    pinned
}

/// Binds the reverse proxy on `ESPRESSO_PROXY_PORT` and returns a handle whose `Drop` aborts the
/// server task. `submit_delay`/`drift_polls` select the perturbation (see [`ProxyState`]).
async fn start_espresso_proxy(submit_delay: Option<Duration>, drift_polls: usize) -> EspressoProxy {
    let state = Arc::new(ProxyState {
        upstream: ESPRESSO_UPSTREAM.to_string(),
        // Pass the node's version redirect (307 /node/.. -> /v1/node/..) through untouched.
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("failed to build proxy client"),
        submit_delay,
        held_first_submit: AtomicBool::new(false),
        drift_polls,
        freeze_height: AtomicU64::new(0),
        freeze_polls_remaining: AtomicUsize::new(0),
        drifted: AtomicBool::new(false),
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

/// Drives CAS through the proxy and waits for `expected_batches` to land on L1, asserting the
/// proxy's perturbation actually engaged. Returns once both hold.
async fn run_cas_through_proxy(proxy: &EspressoProxy, expected_batches: usize) {
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
    )
    .await;

    let probe_url = CasRoute::Calldata.rpc_url_local();
    let cas = spawn_cas_with_retries(&cas_config_path, &probe_url).await;

    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster(
        CAS_FEED_URL,
        CasRoute::Calldata.rpc_url_for_poster(),
        &sequencer_inbox.to_string(),
    );

    wait_for_batches_on_l1(&l1, from_block, expected_batches, sequencer_inbox).await;

    let drifted = proxy.state.drifted.load(Ordering::SeqCst);
    println!("proxy stats: drifted={drifted}");
    assert!(drifted, "proxy never perturbed traffic; scenario did not engage");

    drop(cas);
    drop(nitro_node);
}

// An earlier rollup message is finalized in Espresso *after* later ones (the proxy holds its
// submission). CAS must reorder and still post correct batches to L1 — the original drift test,
// now run against the verifying light client (the out-of-order is real, not a rewritten response).
#[tokio::test]
async fn test_e2e_message_drift() {
    let proxy = start_espresso_proxy(Some(Duration::from_secs(6)), 0).await;
    run_cas_through_proxy(&proxy, 5).await;
    drop(proxy);
}

// The query node lags (its reported block height is pinned for a while). CAS must catch up once
// the true tip is released and still post batches to L1.
#[tokio::test]
async fn test_e2e_lagging_query_node() {
    let proxy = start_espresso_proxy(None, 12).await;
    run_cas_through_proxy(&proxy, 5).await;
    drop(proxy);
}
