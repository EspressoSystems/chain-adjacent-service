use alloy::primitives::{Address, address};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::{Instant, sleep};

use chain_adjacent_service::espresso_e2e::espresso_dev_node::EspressoDevNode;
use chain_adjacent_service::rollups::nitro::l1_monitor::ISequencerInbox;

use crate::cas_harness::setup_l1_reuse_mode_with_cas_poster;
use crate::nitro_node::nitro_node::{NitroNode, NitroNodeConfig};

const CAS_BIN: &str = env!("CARGO_BIN_EXE_chain-adjacent-service");

const CAS_FEED_URL: &str = "ws://host.docker.internal:9643";
const CAS_CALLDATA_RPC_URL: &str = "http://host.docker.internal:8000/cas/arb/calldata";
const CAS_ANYTRUST_RPC_URL: &str = "http://host.docker.internal:8000/cas/arb/anytrust";
const CAS_LOCAL_BASE_URL: &str = "http://localhost:8000";

const ANYTRUST_DAPROVIDER_URL: &str = "http://localhost:9881";

const L1_WS_URL: &str = "ws://localhost:8546";
const SEQUENCER_INBOX: Address = address!("E44f73d4e7b3C008b71CF273000703F5B6380119");

#[derive(Clone, Copy)]
enum CasRoute {
    Calldata,
    Anytrust,
}

impl CasRoute {
    fn rpc_url_for_poster(&self) -> &'static str {
        match self {
            CasRoute::Calldata => CAS_CALLDATA_RPC_URL,
            CasRoute::Anytrust => CAS_ANYTRUST_RPC_URL,
        }
    }

    /// Host-reachable equivalent of `rpc_url_for_poster` for the test's
    /// own readiness probe — same path, but on localhost.
    fn rpc_url_local(&self) -> String {
        let path = match self {
            CasRoute::Calldata => "/cas/arb/calldata",
            CasRoute::Anytrust => "/cas/arb/anytrust",
        };
        format!("{CAS_LOCAL_BASE_URL}{path}")
    }
}

/// RAII wrapper that kills the CAS subprocess on drop so the test never
/// leaks a background process if it panics.
struct CasProcess(Child);

impl Drop for CasProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Anvil/Hardhat account 0 from the "test test ... junk" mnemonic.
const TEST_OPERATOR_PRIVATE_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn spawn_cas(config_path: &Path) -> CasProcess {
    let child = Command::new(CAS_BIN)
        .arg("--config")
        .arg(config_path)
        .env("OPERATOR_PRIVATE_KEY", TEST_OPERATOR_PRIVATE_KEY)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn CAS binary");
    CasProcess(child)
}

/// Spawns CAS and waits for it to become ready on `probe_url`, retrying
/// the whole (spawn -> wait) cycle if CAS exits early or doesn't come up
/// in time.
///
/// CAS startup hits the L1 WS endpoint to fetch the latest checkpoint,
/// and that connection occasionally fails with broken-pipe / network-down
/// from anvil — which currently causes CAS to exit. Retrying side-steps
/// the flake until we add proper retry logic inside CAS itself.
async fn spawn_cas_with_retries(config_path: &Path, probe_url: &str) -> CasProcess {
    const MAX_ATTEMPTS: usize = 5;
    const PER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);

    for attempt in 1..=MAX_ATTEMPTS {
        println!("CAS spawn attempt {attempt}/{MAX_ATTEMPTS}");
        let mut cas = spawn_cas(config_path);

        match tokio::time::timeout(PER_ATTEMPT_TIMEOUT, wait_for_cas_ready(&mut cas, probe_url))
            .await
        {
            Ok(Ok(())) => return cas,
            Ok(Err(reason)) => {
                println!("CAS attempt {attempt} failed: {reason}");
            }
            Err(_) => {
                println!("CAS attempt {attempt} timed out after {PER_ATTEMPT_TIMEOUT:?}");
            }
        }
        // CasProcess::drop kills any child still running.
        drop(cas);
        sleep(Duration::from_secs(2)).await;
    }
    panic!("CAS failed to become ready after {MAX_ATTEMPTS} attempts");
}

/// Polls `probe_url` until CAS answers a JSON-RPC request, or returns
/// `Err` if the subprocess exits before that happens. The caller is
/// expected to apply a timeout via `tokio::time::timeout`.
async fn wait_for_cas_ready(cas: &mut CasProcess, probe_url: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    // The readiness signal is "server is bound and processing requests",
    // not "this specific RPC method succeeds" — see the response-handling
    // arm below for why route-specific status codes are both acceptable.
    let body = json!({
        "jsonrpc": "2.0",
        "method": "daprovider_getSupportedHeaderBytes",
        "params": [],
        "id": 1,
    });

    loop {
        match cas.0.try_wait() {
            Ok(Some(status)) => return Err(format!("CAS exited early with status {status}")),
            Ok(None) => {}
            Err(err) => return Err(format!("try_wait on CAS failed: {err}")),
        }

        match client.post(probe_url).json(&body).send().await {
            // Any HTTP response means CAS is bound and serving on this
            // route. Both calldata (handled locally, 200 OK) and anytrust
            // (forwarded to the sidecar) come up successfully here.
            Ok(resp) => {
                println!(
                    "CAS DA RPC is ready ({probe_url}, status {})",
                    resp.status()
                );
                return Ok(());
            }
            Err(err) => {
                println!("waiting for CAS: {err}");
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
}

/// Same broken-pipe / network-down flake hits the test's own L1 WS
/// connection. Wrap connect with a retry loop.
async fn connect_l1_ws_with_retries() -> RootProvider {
    const MAX_ATTEMPTS: usize = 10;
    let mut last_err: Option<String> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        match RootProvider::connect(L1_WS_URL).await {
            Ok(provider) => return provider,
            Err(err) => {
                println!("L1 connect attempt {attempt}/{MAX_ATTEMPTS} failed: {err}");
                last_err = Some(err.to_string());
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
    panic!(
        "L1 connect failed after {MAX_ATTEMPTS} attempts: {}",
        last_err.unwrap_or_default()
    );
}

const TEE_VERIFIER_STATE_FILE: &str = "tests/nitro/l1_node/state/tee_verifier_address.txt";

fn read_tee_verifier_address() -> Address {
    let content = std::fs::read_to_string(TEE_VERIFIER_STATE_FILE).unwrap_or_else(|_| {
        panic!("missing {TEE_VERIFIER_STATE_FILE} — run setup.sh --init-force")
    });
    content
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("bad address in {TEE_VERIFIER_STATE_FILE}: {e}"))
}

/// Builds the CAS config inline and writes it to a runtime path so each
/// test can inject the right `streamer.starting_hotshot_height` (the
/// Espresso dev node container is reused across runs and accumulates
/// hotshot blocks; without this, CAS would replay stale messages from a
/// prior test against a freshly reset L1).
///
/// For the anytrust route, anytrust is just another entry in
/// `da_providers` — CAS treats it like any other forwarded provider, and
/// the daprovider sidecar handles all of the BLS aggregation / payload
/// recovery work.
///
/// Sets `is_fresh_deployment: true` so `resolve_config_with_checkpoint`
/// preserves the `starting_hotshot_height` we wrote here instead of
/// overwriting it with whatever it scans off L1.
fn write_cas_config(
    starting_hotshot_height: u64,
    tee_verifier_address: Address,
    route: CasRoute,
) -> PathBuf {
    let da_server = match route {
        CasRoute::Calldata => json!({"listen_addr": "0.0.0.0:8000"}),
        CasRoute::Anytrust => json!({
            "listen_addr": "0.0.0.0:8000",
            "da_providers": [
                {
                    "name": "anytrust",
                    "endpoint_url": ANYTRUST_DAPROVIDER_URL,
                    "is_anytrust": true,
                }
            ]
        }),
    };

    let config = json!({
        "espresso_client": {
            "base_url": "http://localhost:41000"
        },
        "streamer": {
            "starting_hotshot_height": starting_hotshot_height,
        },
        "rollup": {
            "type": "nitro",
            "namespace_id": 412346,
            "stack": {
                "chain_id": 412346,
                "feed": {
                    "web_socket_url": "ws://localhost:9642",
                    "current_message_count": 0,
                    "client": {
                        "trusted_sequencer_addresses": [
                            "0xe2148eE53c0755215Df69b2616E552154EdC584f"
                        ]
                    },
                    "server": {
                        "ws_server": {
                            "port": 9643,
                            "enable_compression": true
                        }
                    }
                },
                "l1_ws_url": "ws://localhost:8546",
                "sequencer_inbox_address": "0xE44f73d4e7b3C008b71CF273000703F5B6380119"
            }
        },
        "da_server": da_server,
        "submitter": {
            "max_in_flight": 1000
        },
        "key_manager": {
            "rpc_url": "http://localhost:8545",
            "tee_verifier_address": format!("{tee_verifier_address}"),
            "attestation_verifier_url": "http://localhost:9000",
            "tee_type": "test"
        },
        "is_fresh_deployment": true,
    });

    let path = std::env::temp_dir().join(match route {
        CasRoute::Calldata => "cas-config-e2e-calldata.json",
        CasRoute::Anytrust => "cas-config-e2e-anytrust.json",
    });
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&config).expect("serialize CAS config"),
    )
    .expect("failed to write CAS config");
    path
}

async fn wait_for_batches_on_l1(provider: &RootProvider, from_block: u64, min: usize) {
    let filter = Filter::new()
        .address(SEQUENCER_INBOX)
        .event_signature(ISequencerInbox::SequencerBatchDelivered::SIGNATURE_HASH)
        .from_block(from_block);

    let deadline = Instant::now() + Duration::from_secs(5 * 60);
    loop {
        if Instant::now() >= deadline {
            let count = provider
                .get_logs(&filter)
                .await
                .map(|l| l.len())
                .unwrap_or(0);
            panic!(
                "timed out: only saw {count}/{min} SequencerBatchDelivered events on L1 \
                 from block {from_block}"
            );
        }
        match provider.get_logs(&filter).await {
            Ok(logs) if logs.len() >= min => {
                println!(
                    "observed {} SequencerBatchDelivered events on L1 (target {min})",
                    logs.len()
                );
                return;
            }
            Ok(logs) => {
                println!(
                    "observed {}/{min} SequencerBatchDelivered events on L1",
                    logs.len()
                );
            }
            Err(err) => {
                println!("get_logs failed: {err}");
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
}

/// Drives the full end-to-end pipeline for a given CAS route. Both
/// `test_e2e_calldata` and `test_e2e_anytrust` are thin wrappers around
/// this — the only differences are which URL the poster talks to, which
/// `da_server` block goes into CAS's config, and (for anytrust) whether
/// we need to bring up the DAS committee + daprovider sidecar before the
/// poster starts.
async fn run_e2e(route: CasRoute) {
    let espresso = EspressoDevNode::start().await;
    println!(
        "Espresso dev node started at {}",
        espresso.client.config.base_url
    );

    let anytrust = matches!(route, CasRoute::Anytrust);
    setup_l1_reuse_mode_with_cas_poster(CAS_FEED_URL, route.rpc_url_for_poster(), anytrust);

    let config = NitroNodeConfig {
        no_l2_traffic: false,
        ..Default::default()
    };

    // Phase 1: bring up L1 + sequencer (no poster yet — it's gated behind
    // CAS being reachable on the feed and DA endpoints).
    let nitro_node = NitroNode::start(config).await;
    println!("Nitro node + L1 (reuse mode) started");

    if anytrust {
        nitro_node.start_das_committee();
        println!("DAS committee + mirror started");
        nitro_node.start_anytrust_daprovider();
        println!("daprovider-anytrust sidecar started");
    }

    // Phase 2: snapshot Espresso's current hotshot height so CAS skips
    // historical blocks left behind by prior runs (the dev node container
    // is reused across tests). Then spawn CAS and wait for its DA RPC to
    // come up so the poster has a live endpoint by the time it boots.
    let starting_hotshot_height = espresso
        .client
        .fetch_latest_hotshot_block_height()
        .await
        .expect("failed to fetch latest hotshot block height")
        + 1;
    let tee_verifier_address = read_tee_verifier_address();
    println!("Using TEE verifier mock at {tee_verifier_address}");
    let cas_config_path = write_cas_config(starting_hotshot_height, tee_verifier_address, route);
    println!(
        "CAS config written to {} (starting_hotshot_height={starting_hotshot_height})",
        cas_config_path.display()
    );
    let probe_url = route.rpc_url_local();
    let cas = spawn_cas_with_retries(&cas_config_path, &probe_url).await;

    // Phase 3: snapshot the L1 head so the batch-count assertion ignores
    // anything pre-existing in the anvil snapshot, then bring up the
    // poster.
    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster();
    println!("Poster started");

    // Phase 4: assert the end-to-end pipeline produces at least 5 batches
    wait_for_batches_on_l1(&l1, from_block, 5).await;

    drop(cas);
    drop(nitro_node);
    drop(espresso);
}

#[tokio::test]
async fn test_e2e_calldata() {
    run_e2e(CasRoute::Calldata).await;
}

#[tokio::test]
async fn test_e2e_anytrust() {
    run_e2e(CasRoute::Anytrust).await;
}
