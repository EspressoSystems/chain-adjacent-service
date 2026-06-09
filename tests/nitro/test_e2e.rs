use alloy::eips::BlockNumberOrTag;
use alloy::primitives::{Address, Bytes};
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::Filter;
use alloy::sol;
use alloy::sol_types::SolEvent;
use alloy_rlp::Encodable;
use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::post;
use base64::Engine;
use base64::engine::general_purpose;
use espresso_types::{NamespaceId, Transaction};
use reqwest::Client;
use serde_json::{Value, json};
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

use chain_adjacent_service::espresso_e2e::espresso_dev_node::EspressoDevNode;
use chain_adjacent_service::rollups::nitro::l1_monitor::{
    fetch_message_count, read_bridge_address,
};

sol! {
    event SequencerBatchDelivered(
        uint256 indexed batchSequenceNumber,
        bytes32 indexed beforeAcc,
        bytes32 indexed afterAcc,
        bytes32 delayedAcc,
        uint256 afterDelayedMessagesRead,
        (uint64, uint64, uint64, uint64) timeBounds,
        uint8 dataLocation
    );
}

use crate::nitro_node::nitro_node::{NitroNode, NitroNodeConfig};

const CAS_BIN: &str = env!("CARGO_BIN_EXE_chain-adjacent-service");

pub(crate) const CAS_FEED_URL: &str = "ws://host.docker.internal:9643";
const CAS_CALLDATA_RPC_URL: &str = "http://host.docker.internal:8000/cas/arb/calldata";
const CAS_ANYTRUST_RPC_URL: &str = "http://host.docker.internal:8000/cas/arb/anytrust";
const CAS_EXTERNAL_DA_ANYTRUST_RPC_URL: &str =
    "http://host.docker.internal:8000/cas/arb/external-da-anytrust";
const CAS_LOCAL_BASE_URL: &str = "http://localhost:8000";

const ANYTRUST_DAPROVIDER_URL: &str = "http://localhost:9881";
const EXTERNAL_DA_MOCK_URL: &str = "http://localhost:9882";

const L1_WS_URL: &str = "ws://localhost:8545";
const L1_HTTP_URL: &str = "http://localhost:8545";
const SEQUENCER_HTTP_URL: &str = "http://localhost:8547";
const VALIDATOR_HTTP_URL: &str = "http://localhost:8949";
const GENERATED_CONFIG_DIR: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/nitro/generated-config");
const TRUSTED_SEQUENCER_ADDRESS: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

#[derive(Clone, Copy)]
pub(crate) enum CasRoute {
    Calldata,
    Anytrust,
    ExternalDaAnytrustFallback,
}

impl CasRoute {
    pub(crate) fn rpc_url_for_poster(&self) -> &'static str {
        match self {
            CasRoute::Calldata => CAS_CALLDATA_RPC_URL,
            CasRoute::Anytrust => CAS_ANYTRUST_RPC_URL,
            CasRoute::ExternalDaAnytrustFallback => CAS_EXTERNAL_DA_ANYTRUST_RPC_URL,
        }
    }

    pub(crate) fn rpc_url_local(&self) -> String {
        let path = match self {
            CasRoute::Calldata => "/cas/arb/calldata",
            CasRoute::Anytrust => "/cas/arb/anytrust",
            CasRoute::ExternalDaAnytrustFallback => "/cas/arb/external-da-anytrust",
        };
        format!("{CAS_LOCAL_BASE_URL}{path}")
    }
}

pub(crate) struct CasProcess(Child);

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
    spawn_cas_with_stdio(config_path, Stdio::inherit(), Stdio::inherit())
}

fn spawn_cas_to_file(config_path: &Path, log_path: &Path) -> CasProcess {
    let stdout = std::fs::File::create(log_path)
        .unwrap_or_else(|e| panic!("create CAS log file {}: {e}", log_path.display()));
    let stderr = stdout
        .try_clone()
        .expect("clone CAS log file handle for stderr");
    spawn_cas_with_stdio(config_path, Stdio::from(stdout), Stdio::from(stderr))
}

fn spawn_cas_with_stdio(config_path: &Path, stdout: Stdio, stderr: Stdio) -> CasProcess {
    let child = Command::new(CAS_BIN)
        .arg("--config")
        .arg(config_path)
        .env("OPERATOR_PRIVATE_KEY", TEST_OPERATOR_PRIVATE_KEY)
        .env("RUST_LOG", "warn")
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .expect("failed to spawn CAS binary");
    CasProcess(child)
}

async fn spawn_cas_with_retries_inner<F: FnMut() -> CasProcess>(
    mut spawn: F,
    probe_url: &str,
) -> CasProcess {
    const MAX_ATTEMPTS: usize = 5;
    const PER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);

    for attempt in 1..=MAX_ATTEMPTS {
        println!("CAS spawn attempt {attempt}/{MAX_ATTEMPTS}");
        let mut cas = spawn();

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
        drop(cas);
        sleep(Duration::from_secs(2)).await;
    }
    panic!("CAS failed to become ready after {MAX_ATTEMPTS} attempts");
}

pub(crate) async fn spawn_cas_with_retries(config_path: &Path, probe_url: &str) -> CasProcess {
    spawn_cas_with_retries_inner(|| spawn_cas(config_path), probe_url).await
}

pub(crate) async fn spawn_cas_with_retries_to_file(
    config_path: &Path,
    probe_url: &str,
    log_path: &Path,
) -> CasProcess {
    spawn_cas_with_retries_inner(|| spawn_cas_to_file(config_path, log_path), probe_url).await
}

async fn wait_for_cas_ready(cas: &mut CasProcess, probe_url: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
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

pub(crate) async fn connect_l1_ws_with_retries() -> RootProvider {
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

async fn evm_snapshot() -> String {
    let client = reqwest::Client::new();
    let body = json!({
        "jsonrpc": "2.0",
        "method": "evm_snapshot",
        "params": [],
        "id": 1,
    });
    let resp: serde_json::Value = client
        .post(L1_HTTP_URL)
        .json(&body)
        .send()
        .await
        .expect("evm_snapshot RPC failed")
        .json()
        .await
        .expect("evm_snapshot response parse failed");
    resp["result"]
        .as_str()
        .expect("evm_snapshot result not a string")
        .to_string()
}

/// Mine `count` empty blocks instantly on Anvil.
///
/// The loaded L1 state has `historical_states: null`, so Anvil cannot serve
/// `eth_call` at blocks that existed only in the loaded snapshot. By mining
/// enough blocks we push the "finalized" tag (≈ latest − 64) past the
/// loaded-state boundary, ensuring the CAS L1 monitor can read contract
/// state at the finalized block.
async fn anvil_mine(count: u64) {
    let client = reqwest::Client::new();
    let body = json!({
        "jsonrpc": "2.0",
        "method": "anvil_mine",
        "params": [count],
        "id": 1,
    });
    let resp: serde_json::Value = client
        .post(L1_HTTP_URL)
        .json(&body)
        .send()
        .await
        .expect("anvil_mine RPC failed")
        .json()
        .await
        .expect("anvil_mine response parse failed");
    assert!(
        resp.get("error").is_none(),
        "anvil_mine returned error: {resp}"
    );
}

// Anvil consumes the snapshot on revert — it cannot be reused.
async fn evm_revert(snapshot_id: &str) -> bool {
    let client = reqwest::Client::new();
    let body = json!({
        "jsonrpc": "2.0",
        "method": "evm_revert",
        "params": [snapshot_id],
        "id": 1,
    });
    let resp: serde_json::Value = client
        .post(L1_HTTP_URL)
        .json(&body)
        .send()
        .await
        .expect("evm_revert RPC failed")
        .json()
        .await
        .expect("evm_revert response parse failed");
    resp["result"].as_bool().unwrap_or(false)
}

pub(crate) fn read_tee_verifier_address() -> Address {
    let path = Path::new(GENERATED_CONFIG_DIR).join("tee_verifier_address.txt");
    let content = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing {} — run docker compose with deploy profile first",
            path.display()
        )
    });
    content
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("bad address in {}: {e}", path.display()))
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
#[allow(clippy::too_many_arguments)]
/// Build the light-client genesis from the dev node's `/config/hotshot` (stake table + epoch
/// params). The e2e dev node has a static validator set, so the genesis is fixed;
/// `first_epoch = epoch_start_block / epoch_height + 3`.
async fn devnode_light_client_genesis(base_url: &str) -> serde_json::Value {
    let url = format!("{}/config/hotshot", base_url.trim_end_matches('/'));
    let cfg: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("fetch /config/hotshot")
        .json()
        .await
        .expect("parse /config/hotshot");
    let c = &cfg["config"];
    let epoch_height = c["epoch_height"].as_u64().expect("config.epoch_height");
    let epoch_start_block = c["epoch_start_block"]
        .as_u64()
        .expect("config.epoch_start_block");
    let stake_table: Vec<serde_json::Value> = c["known_nodes_with_stake"]
        .as_array()
        .expect("config.known_nodes_with_stake")
        .iter()
        .map(|n| n["stake_table_entry"].clone())
        .collect();
    json!({
        "epoch_height": epoch_height,
        "first_epoch_with_dynamic_stake_table": epoch_start_block / epoch_height + 3,
        "stake_table": stake_table,
    })
}

pub(crate) async fn write_cas_config(
    starting_hotshot_height: u64,
    route: CasRoute,
    sequencer_inbox: &Address,
    tee_verifier_address: Address,
    is_fresh_deployment: bool,
    client_timeout_secs: Option<u64>,
    espresso_base_url_override: Option<&str>,
    max_full_queue_entries: Option<usize>,
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
        CasRoute::ExternalDaAnytrustFallback => json!({
            "listen_addr": "0.0.0.0:8000",
            // Set the calldata max size to an extremely low value, which
            // make the poster impossible to post batches through calldata path
            "calldata_max_size": 1,
            "da_providers": [
                {
                    "name": "external-da-anytrust",
                    "endpoint_url": EXTERNAL_DA_MOCK_URL,
                    "is_anytrust": false,
                    "anytrust_fallback_url": ANYTRUST_DAPROVIDER_URL,
                }
            ]
        }),
    };

    let base_url = espresso_base_url_override.unwrap_or("http://localhost:41000");
    let mut espresso_client = json!({ "base_url": base_url });
    if let Some(timeout) = client_timeout_secs {
        espresso_client["client_timeout_secs"] = json!(timeout);
    }
    let light_client = json!({ "genesis": devnode_light_client_genesis(base_url).await });

    let mut streamer = json!({
        "starting_hotshot_height": starting_hotshot_height,
    });
    if let Some(cap) = max_full_queue_entries {
        streamer["max_full_queue_entries"] = json!(cap);
    }

    let config = json!({
        "espresso_client": espresso_client,
        "light_client": light_client,
        "streamer": streamer,
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
                            TRUSTED_SEQUENCER_ADDRESS
                        ]
                    },
                    "server": {
                        "ws_server": {
                            "port": 9643,
                            "enable_compression": true
                        }
                    }
                },
                "l1_http_url": L1_HTTP_URL,
                "l1_ws_url": L1_WS_URL,
                "sequencer_inbox_address": sequencer_inbox.to_string()
            }
        },
        "da_server": da_server,
        "submitter": {
            "max_in_flight": 1000
        },
        "key_manager": {
            "tee_verifier_address": format!("{tee_verifier_address}"),
            "attestation_verifier_url": "http://localhost:9000",
            "tee_type": "test"
        },
        "is_fresh_deployment": is_fresh_deployment,
    });

    let path = std::env::temp_dir().join(match route {
        CasRoute::Calldata => "cas-config-e2e-calldata.json",
        CasRoute::Anytrust => "cas-config-e2e-anytrust.json",
        CasRoute::ExternalDaAnytrustFallback => "cas-config-e2e-external-da-anytrust-fallback.json",
    });
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&config).expect("serialize CAS config"),
    )
    .expect("failed to write CAS config");
    path
}

// https://github.com/EspressoSystems/nitro-contracts/blob/ec47f8578cc0837347af9de5bf6c34ed3e037d93/src/rollup/IRollupCore.sol#L26
const ASSERTION_CREATED_TOPIC: alloy::primitives::B256 =
    alloy::primitives::b256!("901c3aee23cf4478825462caaab375c606ab83516060388344f0650340753630");

fn read_rollup_address() -> Address {
    let path = Path::new(GENERATED_CONFIG_DIR).join("deployment.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
    let deployment: serde_json::Value = serde_json::from_str(&raw)
        .unwrap_or_else(|err| panic!("failed to parse {}: {err}", path.display()));
    let value = deployment
        .get("rollup")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("missing rollup in {}", path.display()));
    value
        .parse()
        .unwrap_or_else(|err| panic!("invalid rollup address {value}: {err}"))
}

async fn wait_for_assertion_created(provider: &RootProvider, rollup: Address, from_block: u64) {
    let filter = Filter::new()
        .address(rollup)
        .event_signature(ASSERTION_CREATED_TOPIC)
        .from_block(from_block);

    let deadline = Instant::now() + Duration::from_secs(5 * 60);
    loop {
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for AssertionCreated on rollup {rollup} from block {from_block}"
            );
        }
        match provider.get_logs(&filter).await {
            Ok(logs) if !logs.is_empty() => {
                println!(
                    "staker active: {} AssertionCreated event(s) on rollup {rollup}",
                    logs.len()
                );
                return;
            }
            Ok(_) => {
                println!("waiting for AssertionCreated on rollup {rollup}...");
            }
            Err(err) => {
                println!("get_logs failed: {err}");
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}

pub(crate) fn read_sequencer_inbox_address() -> Address {
    let path = Path::new(GENERATED_CONFIG_DIR).join("deployment.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
    let deployment: serde_json::Value = serde_json::from_str(&raw)
        .unwrap_or_else(|err| panic!("failed to parse {}: {err}", path.display()));
    let value = deployment
        .get("sequencer-inbox")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("missing sequencer-inbox in {}", path.display()));
    value
        .parse()
        .unwrap_or_else(|err| panic!("invalid sequencer-inbox address {value}: {err}"))
}

async fn count_batches_on_l1(
    provider: &RootProvider,
    from_block: u64,
    sequencer_inbox: Address,
) -> usize {
    let filter = Filter::new()
        .address(sequencer_inbox)
        .event_signature(SequencerBatchDelivered::SIGNATURE_HASH)
        .from_block(from_block);
    provider
        .get_logs(&filter)
        .await
        .map(|l| l.len())
        .unwrap_or(0)
}

fn connect_http(url: &str) -> impl Provider + Clone {
    ProviderBuilder::new().connect_http(
        url.parse::<url::Url>()
            .unwrap_or_else(|e| panic!("failed to parse {url}: {e}")),
    )
}

async fn wait_for_validator_to_reach(validator: &impl Provider, target: u64) {
    let deadline = Instant::now() + Duration::from_secs(3 * 60);
    loop {
        let current = validator
            .get_block_number()
            .await
            .unwrap_or_else(|e| panic!("failed to get validator block number: {e}"));
        if current >= target {
            println!("validator caught up: block {current} >= target {target}");
            return;
        }
        if Instant::now() >= deadline {
            panic!("timed out: validator at block {current}, target {target}");
        }
        println!("validator at block {current}, waiting to reach {target}");
        sleep(Duration::from_secs(2)).await;
    }
}

pub(crate) async fn wait_for_batches_on_l1(
    provider: &RootProvider,
    from_block: u64,
    min: usize,
    sequencer_inbox: Address,
) {
    let filter = Filter::new()
        .address(sequencer_inbox)
        .event_signature(SequencerBatchDelivered::SIGNATURE_HASH)
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

// Batch wire format: [0x00 brotli header] [brotli-compressed RLP segments]
// Each segment: [kind_byte | payload], where kind 0x00 = L2 message.
fn build_malicious_batch() -> Bytes {
    let mut segment = vec![0x00u8]; // 0x00 = BATCH_SEGMENT_KIND_L2_MESSAGE
    segment.extend_from_slice(b"malicious_tx_data_not_in_espresso");

    let rlp_bytes = alloy_rlp::Bytes::from(segment);
    let mut rlp_data = Vec::new();
    rlp_bytes.encode(&mut rlp_data);

    let params = brotli::enc::BrotliEncoderParams {
        quality: 0,
        ..Default::default()
    };
    let mut compressed = Vec::new();
    brotli::BrotliCompress(&mut Cursor::new(&rlp_data), &mut compressed, &params)
        .expect("brotli compression should not fail on valid input");

    let mut batch = Vec::with_capacity(1 + compressed.len());
    batch.push(0x00); // 0x00 = BROTLI_HEADER_BYTE
    batch.extend_from_slice(&compressed);

    Bytes::from(batch)
}

// Sends a raw batch to the CAS DA RPC endpoint, bypassing the normal poster flow.
// Uses the `daprovider_store` method, which is used by the batch poster to submit a batch to CAS
async fn send_store_rpc(url: &str, data: Bytes) -> Value {
    let client = Client::new();
    let body = json!({
        "jsonrpc": "2.0",
        "method": "daprovider_store",
        "params": [data, 5000u64],
        "id": 1,
    });

    client
        .post(url)
        .json(&body)
        .send()
        .await
        .expect("daprovider_store RPC failed")
        .json()
        .await
        .expect("daprovider_store response parse failed")
}

async fn run_e2e(route: CasRoute) {
    let config = NitroNodeConfig {
        no_l2_traffic: false,
    };

    let nitro_node = NitroNode::start(config).await;
    println!("Nitro stack started (L1 + sequencer + espresso dev node)");

    let sequencer_inbox = read_sequencer_inbox_address();
    let anytrust = matches!(route, CasRoute::Anytrust);

    if anytrust {
        nitro_node.start_das_committee();
        println!("DAS committee + mirror started");
        nitro_node.start_anytrust_daprovider(&sequencer_inbox.to_string());
        println!("daprovider-anytrust sidecar started");
    }

    let espresso = EspressoDevNode::connect().await;
    println!(
        "Espresso dev node ready at {}",
        espresso.client.config.base_url
    );

    let starting_hotshot_height = espresso
        .client
        .fetch_latest_hotshot_block_height()
        .await
        .expect("failed to fetch latest hotshot block height")
        + 1;
    let tee_verifier_address = read_tee_verifier_address();
    println!("Using TEE verifier mock at {tee_verifier_address}");
    let cas_config_path = write_cas_config(
        starting_hotshot_height,
        route,
        &sequencer_inbox,
        tee_verifier_address,
        true,
        None,
        None,
        None,
    )
    .await;
    println!(
        "CAS config written to {} (starting_hotshot_height={starting_hotshot_height})",
        cas_config_path.display()
    );
    let probe_url = route.rpc_url_local();
    let cas = spawn_cas_with_retries(&cas_config_path, &probe_url).await;

    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster(CAS_FEED_URL, route.rpc_url_for_poster());
    println!("Poster started");

    wait_for_batches_on_l1(&l1, from_block, 2, sequencer_inbox).await;

    nitro_node.stop_tx_generator();
    println!("Load generator stopped; waiting for chain to settle");
    sleep(Duration::from_secs(3)).await;

    let sequencer = connect_http(SEQUENCER_HTTP_URL);
    let sequencer_block = sequencer
        .get_block_number()
        .await
        .expect("failed to get sequencer block number");
    println!("Sequencer at block {sequencer_block}");

    println!("Starting block validator...");
    let rollup = read_rollup_address();
    nitro_node.start_block_validator();

    let validator = connect_http(VALIDATOR_HTTP_URL);
    wait_for_validator_to_reach(&validator, sequencer_block).await;
    let validator_block = validator
        .get_block_number()
        .await
        .expect("failed to get validator block number");
    println!("Block validator at block {validator_block} (sequencer was {sequencer_block})");

    let sequencer_hash = sequencer
        .get_block_by_number(sequencer_block.into())
        .await
        .expect("failed to get sequencer block")
        .unwrap_or_else(|| panic!("sequencer block {sequencer_block} not found"))
        .header
        .hash;
    let validator_hash = validator
        .get_block_by_number(sequencer_block.into())
        .await
        .expect("failed to get validator block")
        .unwrap_or_else(|| panic!("validator block {sequencer_block} not found"))
        .header
        .hash;
    assert_eq!(
        sequencer_hash, validator_hash,
        "block hash mismatch at block {sequencer_block}: sequencer={sequencer_hash}, validator={validator_hash}"
    );
    println!("Block hashes match at block {sequencer_block}: {sequencer_hash}");

    wait_for_assertion_created(&l1, rollup, from_block).await;

    drop(cas);
    drop(nitro_node);
}

#[tokio::test]
async fn test_e2e_calldata() {
    run_e2e(CasRoute::Calldata).await;
}

#[tokio::test]
async fn test_e2e_anytrust() {
    run_e2e(CasRoute::Anytrust).await;
}

#[tokio::test]
async fn test_e2e_l1_reorg() {
    let config = NitroNodeConfig {
        no_l2_traffic: false,
    };

    let nitro_node = NitroNode::start(config).await;
    let espresso = EspressoDevNode::connect().await;

    let starting_hotshot_height = espresso
        .client
        .fetch_latest_hotshot_block_height()
        .await
        .expect("failed to fetch latest hotshot block height");

    let sequencer_inbox = read_sequencer_inbox_address();

    let tee_verifier_address = read_tee_verifier_address();

    let cas_config_path = write_cas_config(
        starting_hotshot_height,
        CasRoute::Calldata,
        &sequencer_inbox,
        tee_verifier_address,
        true,
        None,
        None,
        None,
    )
    .await;

    let probe_url = CasRoute::Calldata.rpc_url_local();
    let cas = spawn_cas_with_retries(&cas_config_path, &probe_url).await;

    let l1 = connect_l1_ws_with_retries().await;

    let bridge = read_bridge_address(&l1, sequencer_inbox)
        .await
        .expect("bridge() call failed");

    let initial_message_count = fetch_message_count(&l1, bridge, BlockNumberOrTag::Latest)
        .await
        .expect("fetch_message_count failed");

    println!("Initial message count on bridge: {initial_message_count}");

    // Anvil's loaded state (block 145) has no historical_states, so eth_call
    // at historical blocks fails. Mine enough blocks so that the "finalized"
    // tag (≈ latest − 64) lands inside Anvil's live-mined range.
    let pre_mine_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 block number");
    let blocks_to_mine = 100u64.saturating_sub(pre_mine_block.saturating_sub(145));
    if blocks_to_mine > 0 {
        anvil_mine(blocks_to_mine).await;
        let post_mine_block = l1
            .get_block_number()
            .await
            .expect("failed to read L1 block number after mining");
        println!("Mined {blocks_to_mine} empty blocks ({pre_mine_block} → {post_mine_block})");
    }

    // We create a snapshot of L1 state before any batch has been posted to L1
    let snapshot_id = evm_snapshot().await;
    println!("EVM snapshot taken: {snapshot_id}");

    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    // Now after taking the snapshot we start the batch poster
    nitro_node.start_poster(CAS_FEED_URL, CasRoute::Calldata.rpc_url_for_poster());
    println!("Poster started");

    // We wait for at least 1 batch to be posted on L1
    wait_for_batches_on_l1(&l1, from_block, 1, sequencer_inbox).await;

    let pre_reorg_message_count = fetch_message_count(&l1, bridge, BlockNumberOrTag::Latest)
        .await
        .expect("fetch_message_count failed");
    println!("Message count on bridge before reorg: {pre_reorg_message_count}");
    assert!(
        pre_reorg_message_count > initial_message_count,
        "expected at least 1 new message on bridge after poster start"
    );

    // Now we revert to the snapshot, which should cause the batch posted above to be reorged out
    let revert_success = evm_revert(&snapshot_id).await;
    assert!(revert_success, "evm_revert failed");
    println!("EVM reverted to snapshot {snapshot_id}");

    let post_reorg_message_count = fetch_message_count(&l1, bridge, BlockNumberOrTag::Latest)
        .await
        .expect("fetch_message_count failed");
    println!("Message count on bridge after reorg: {post_reorg_message_count}");
    assert_eq!(
        post_reorg_message_count, initial_message_count,
        "message count did not revery: got={post_reorg_message_count}, expected={initial_message_count}"
    );

    let revert_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 block number after revert");
    println!("Waiting for batch resubmission from block {revert_block}...");

    // Now we wait for the poster to resubmit the batch that got reorged out,
    // which should cause the message count to go back up to at least
    // what it was before the reorg or higher
    wait_for_batches_on_l1(&l1, revert_block, 1, sequencer_inbox).await;

    let post_resubmit_msg_count = fetch_message_count(&l1, bridge, BlockNumberOrTag::Latest)
        .await
        .expect("fetch_message_count failed");
    assert!(
        post_resubmit_msg_count >= pre_reorg_message_count,
        "message count did not recover after resubmission: \
         got={post_resubmit_msg_count}, pre_reorg={pre_reorg_message_count}"
    );
    println!(
        "Post-resubmission message count: {post_resubmit_msg_count} \
         (recovered from {initial_message_count})"
    );

    drop(cas);
    drop(nitro_node);
}

/// Tests that batch submission pauses when CAS or the poster goes down, and
/// resumes correctly after each component restarts.
#[tokio::test]
async fn test_e2e_restart() {
    let route = CasRoute::Calldata;

    let config = NitroNodeConfig {
        no_l2_traffic: false,
    };
    let nitro_node = NitroNode::start(config).await;
    println!("Nitro stack started (L1 + sequencer + espresso dev node)");

    let sequencer_inbox = read_sequencer_inbox_address();
    let tee_verifier_address = read_tee_verifier_address();

    let espresso = EspressoDevNode::connect().await;
    let starting_hotshot_height = espresso
        .client
        .fetch_latest_hotshot_block_height()
        .await
        .expect("failed to fetch latest hotshot block height")
        + 1;

    let cas_config_path = write_cas_config(
        starting_hotshot_height,
        route,
        &sequencer_inbox,
        tee_verifier_address,
        true,
        None,
        None,
        None,
    )
    .await;
    println!(
        "CAS config written to {} (starting_hotshot_height={starting_hotshot_height})",
        cas_config_path.display()
    );

    let probe_url = route.rpc_url_local();
    let cas = spawn_cas_with_retries(&cas_config_path, &probe_url).await;

    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster(CAS_FEED_URL, route.rpc_url_for_poster());
    println!("Poster started");

    println!("Waiting for 2 batches...");
    wait_for_batches_on_l1(&l1, from_block, 2, sequencer_inbox).await;
    println!("2 batches confirmed on L1");

    println!("Stopping CAS (simulating CAS downtime)...");
    drop(cas);
    println!("CAS stopped; sleeping 30 s to verify no new batches are submitted");
    sleep(Duration::from_secs(30)).await;
    let count_after_cas_down = count_batches_on_l1(&l1, from_block, sequencer_inbox).await;
    assert!(
        count_after_cas_down < 4,
        "expected fewer than 4 batches while CAS was down, got {count_after_cas_down}"
    );
    println!("Confirmed: only {count_after_cas_down} batches during CAS downtime (< 4)");

    println!(
        "Restarting CAS (is_fresh_deployment=false, exercises EspressoBatchVerified recovery)..."
    );
    let cas_restart_config_path = write_cas_config(
        starting_hotshot_height,
        route,
        &sequencer_inbox,
        tee_verifier_address,
        false,
        None,
        None,
        None,
    )
    .await;
    let cas = spawn_cas_with_retries(&cas_restart_config_path, &probe_url).await;
    println!("CAS restarted; waiting for batch count to reach 4...");
    wait_for_batches_on_l1(&l1, from_block, 4, sequencer_inbox).await;
    println!("4 batches confirmed on L1");

    println!("Stopping poster (simulating nitro-node downtime)...");
    nitro_node.stop_poster();
    println!("Poster stopped; sleeping 30 s to verify no new batches are submitted");
    sleep(Duration::from_secs(30)).await;
    let count_after_poster_down = count_batches_on_l1(&l1, from_block, sequencer_inbox).await;
    assert!(
        count_after_poster_down < 6,
        "expected fewer than 6 batches while poster was down, got {count_after_poster_down}"
    );
    println!("Confirmed: only {count_after_poster_down} batches during poster downtime (< 6)");

    println!("Restarting poster...");
    nitro_node.start_poster(CAS_FEED_URL, route.rpc_url_for_poster());
    println!("Poster restarted; waiting for batch count to reach 6...");
    wait_for_batches_on_l1(&l1, from_block, 6, sequencer_inbox).await;
    println!("6 batches confirmed on L1 — restart test passed");

    drop(cas);
    drop(nitro_node);
}

struct MockDaProvider {
    handle: JoinHandle<()>,
    store_count: Arc<AtomicUsize>,
}

impl Drop for MockDaProvider {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn mock_handler(
    State(store_count): State<Arc<AtomicUsize>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let id = parsed.get("id").cloned().unwrap_or(json!(1));
    let method = parsed.get("method").and_then(Value::as_str).unwrap_or("");

    let resp = match method {
        "daprovider_store" => {
            store_count.fetch_add(1, Ordering::SeqCst);
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32000,
                    "message": "DA provider requests fallback to next writer: mock always falls back",
                },
            })
        }
        "daprovider_getMaxMessageSize" => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": { "maxSize": 100_000u64 },
        }),
        "daprovider_recoverPayload" => {
            let cert_hex = parsed["params"][2].as_str().unwrap_or("0x");
            let cert =
                hex::decode(cert_hex.strip_prefix("0x").unwrap_or(cert_hex)).unwrap_or_default();
            const SEQUENCER_HEADER_LEN: usize = 40;
            let payload = if cert.len() > SEQUENCER_HEADER_LEN {
                &cert[SEQUENCER_HEADER_LEN..]
            } else {
                &[][..]
            };
            use base64::Engine;
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "Payload": base64::engine::general_purpose::STANDARD.encode(payload),
                },
            })
        }
        "daprovider_getSupportedHeaderBytes" => {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {"headerBytes": "0x71"},
            })
        }
        _ => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {},
        }),
    };

    axum::Json(resp).into_response()
}

async fn start_mock_da_provider(addr: SocketAddr) -> MockDaProvider {
    let store_count = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/", post(mock_handler))
        .with_state(store_count.clone());

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind mock DA provider on {addr}: {e}"));
    println!("Mock DA provider listening on {addr}");

    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    MockDaProvider {
        handle,
        store_count,
    }
}

#[tokio::test]
async fn test_e2e_anytrust_fallback_to_calldata() {
    let mock_addr: SocketAddr = "127.0.0.1:9881".parse().unwrap();
    let mock = start_mock_da_provider(mock_addr).await;

    let config = NitroNodeConfig {
        no_l2_traffic: false,
    };
    let nitro_node = NitroNode::start(config).await;
    println!("Nitro stack started (L1 + sequencer + espresso dev node)");

    let sequencer_inbox = read_sequencer_inbox_address();
    let espresso = EspressoDevNode::connect().await;

    let starting_hotshot_height = espresso
        .client
        .fetch_latest_hotshot_block_height()
        .await
        .expect("failed to fetch latest hotshot block height")
        + 1;
    let tee_verifier_address = read_tee_verifier_address();

    let cas_config_path = write_cas_config(
        starting_hotshot_height,
        CasRoute::Anytrust,
        &sequencer_inbox,
        tee_verifier_address,
        true,
        None,
        None,
        None,
    )
    .await;

    let probe_url = CasRoute::Anytrust.rpc_url_local();
    let cas = spawn_cas_with_retries(&cas_config_path, &probe_url).await;

    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster(CAS_FEED_URL, CasRoute::Anytrust.rpc_url_for_poster());
    println!("Poster started (anytrust route, mock DA always falls back)");

    wait_for_batches_on_l1(&l1, from_block, 2, sequencer_inbox).await;

    let store_calls = mock.store_count.load(Ordering::SeqCst);
    assert!(
        store_calls >= 2,
        "expected mock DA to receive >=2 daprovider_store calls, got {store_calls}"
    );
    println!("Mock DA observed {store_calls} daprovider_store calls (all rejected with fallback)");

    drop(cas);
    drop(nitro_node);
    drop(mock);
}

#[tokio::test]
async fn test_e2e_espresso_downtime() {
    let route = CasRoute::Calldata;
    let config = NitroNodeConfig {
        no_l2_traffic: false,
    };
    let nitro_node = NitroNode::start(config).await;
    println!("Nitro stack started (L1 + sequencer + espresso dev node)");

    let sequencer_inbox = read_sequencer_inbox_address();
    let tee_verifier_address = read_tee_verifier_address();

    let starting_hotshot_height = {
        let espresso = EspressoDevNode::connect().await;
        espresso
            .client
            .fetch_latest_hotshot_block_height()
            .await
            .expect("failed to fetch latest hotshot block height")
            + 1
    };

    let cas_config_path = write_cas_config(
        starting_hotshot_height,
        route,
        &sequencer_inbox,
        tee_verifier_address,
        true,
        Some(1),
        None,
        None,
    )
    .await;
    let probe_url = route.rpc_url_local();
    let mut cas = spawn_cas_with_retries(&cas_config_path, &probe_url).await;

    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster(CAS_FEED_URL, route.rpc_url_for_poster());
    println!("Poster started; waiting for 1 batch...");
    wait_for_batches_on_l1(&l1, from_block, 1, sequencer_inbox).await;
    println!("1 batch confirmed on L1");

    let pre_fault_batches = count_batches_on_l1(&l1, from_block, sequencer_inbox).await;
    println!("Batch count before Espresso unavailability: {pre_fault_batches}");

    println!("Pausing Espresso dev node to simulate downtime...");
    nitro_node.pause_espresso_dev_node();
    println!("Espresso dev node paused; sleeping 3 s to verify CAS stays alive and retries");
    sleep(Duration::from_secs(3)).await;

    assert!(
        cas.0
            .try_wait()
            .expect("failed to check CAS process status")
            .is_none(),
        "CAS exited while Espresso was unavailable"
    );

    let probe_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "daprovider_getSupportedHeaderBytes",
        "params": [],
        "id": 1,
    });
    let probe_response = Client::new()
        .post(&probe_url)
        .json(&probe_body)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("CAS DA RPC should stay available during Espresso downtime");
    assert!(
        probe_response.status().is_success(),
        "CAS DA RPC returned unexpected status during Espresso downtime: {}",
        probe_response.status()
    );

    let during_fault_batches = count_batches_on_l1(&l1, from_block, sequencer_inbox).await;
    // We allow 1 batch to be posted during the fault window to account for any batch that may have just started processing before the pause took effect
    // but we assert that batch production did not continue beyond that.
    assert!(
        during_fault_batches <= pre_fault_batches + 1,
        "expected batch production to stall while Espresso was down, got {during_fault_batches} batches after starting from {pre_fault_batches}"
    );
    println!(
        "Confirmed: batch production stalled during Espresso downtime ({pre_fault_batches} -> {during_fault_batches})"
    );

    println!("Unpausing Espresso dev node...");
    nitro_node.unpause_espresso_dev_node();
    let target_batches = during_fault_batches + 1;
    println!("Espresso dev node unpaused; waiting for batch count to reach {target_batches}",);
    wait_for_batches_on_l1(&l1, from_block, target_batches, sequencer_inbox).await;
    println!("Batch count reached {target_batches} after Espresso unpause — recovery test passed",);

    drop(cas);
    drop(nitro_node);
}

#[tokio::test]
async fn test_e2e_malicious_batch_poster() {
    let config = NitroNodeConfig {
        no_l2_traffic: false,
    };
    let nitro_node = NitroNode::start(config).await;
    println!("Nitro stack started (L1 + sequencer + espresso dev node)");

    let sequencer_inbox = read_sequencer_inbox_address();
    let espresso = EspressoDevNode::connect().await;

    let starting_hotshot_height = espresso
        .client
        .fetch_latest_hotshot_block_height()
        .await
        .expect("failed to fetch latest hotshot block height")
        + 1;
    let tee_verifier_address = read_tee_verifier_address();

    let cas_config_path = write_cas_config(
        starting_hotshot_height,
        CasRoute::Calldata,
        &sequencer_inbox,
        tee_verifier_address,
        true,
        None,
        None,
        None,
    )
    .await;

    let probe_url = CasRoute::Calldata.rpc_url_local();
    let cas = spawn_cas_with_retries(&cas_config_path, &probe_url).await;

    let l1 = connect_l1_ws_with_retries().await;

    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    let malicious_batch = build_malicious_batch();
    let cas_url = CasRoute::Calldata.rpc_url_local();
    println!("Sending malicious dataprovide_store to {cas_url}");

    // We send a malicious batch to CAS using the same `daprovider_store` RPC method
    // that the poster uses which should cause CAS to attempt to process the batch
    let response = send_store_rpc(&cas_url, malicious_batch).await;
    println!("Received response from CAS: {response}");

    assert!(
        response.get("error").is_some(),
        "expected CAS to reject the malicious batch with an error, got: {response}"
    );
    let error_msg = response["error"]["message"]
        .as_str()
        .unwrap_or("<no message>");
    assert!(
        error_msg.contains("CAS verification failed"),
        "expected 'CAS verification failed' error, got: {error_msg}"
    );
    println!("CAS correctly rejected malicious batch: {error_msg}");

    sleep(Duration::from_secs(5)).await;
    let batch_count_after = count_batches_on_l1(&l1, from_block, sequencer_inbox).await;
    assert_eq!(
        batch_count_after, 0,
        "malicious batch must not reach L1: batch_count={batch_count_after}"
    );
    println!("Confirmed: no new batches on L1 after malicious attempt");

    drop(cas);
    drop(nitro_node);
}

#[tokio::test]
async fn test_e2e_external_da_fallback_to_anytrust() {
    let route = CasRoute::ExternalDaAnytrustFallback;
    let mock_addr: SocketAddr = "127.0.0.1:9882".parse().unwrap();
    let mock = start_mock_da_provider(mock_addr).await;

    let nitro_node = NitroNode::start(NitroNodeConfig {
        no_l2_traffic: false,
    })
    .await;
    println!("Nitro stack started");

    let sequencer_inbox = read_sequencer_inbox_address();
    nitro_node.start_das_committee();
    nitro_node.start_anytrust_daprovider(&sequencer_inbox.to_string());
    println!("anytrust sidecar started on {ANYTRUST_DAPROVIDER_URL}");

    let espresso = EspressoDevNode::connect().await;
    let starting_hotshot_height = espresso
        .client
        .fetch_latest_hotshot_block_height()
        .await
        .expect("failed to fetch latest hotshot block height")
        + 1;
    let tee_verifier_address = read_tee_verifier_address();

    let cas_config_path = write_cas_config(
        starting_hotshot_height,
        route,
        &sequencer_inbox,
        tee_verifier_address,
        true,
        None,
        None,
        None,
    )
    .await;

    let probe_url = route.rpc_url_local();
    let cas_log_path = std::env::temp_dir().join("cas-e2e-external-da-anytrust-fallback.log");
    let cas = spawn_cas_with_retries_to_file(&cas_config_path, &probe_url, &cas_log_path).await;

    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster(CAS_FEED_URL, route.rpc_url_for_poster());
    println!("Poster started (external-da route, mock primary falls back to anytrust)");

    wait_for_batches_on_l1(&l1, from_block, 2, sequencer_inbox).await;

    let store_calls = mock.store_count.load(Ordering::SeqCst);
    assert!(
        store_calls >= 2,
        "expected mock primary to receive >=2 daprovider_store calls, got {store_calls}"
    );

    drop(cas);

    let cas_logs = std::fs::read_to_string(&cas_log_path)
        .unwrap_or_else(|e| panic!("read CAS log {}: {e}", cas_log_path.display()));
    let fallback_line = "primary DA requested fallback; trying anytrust sidecar";
    assert!(
        cas_logs.contains(fallback_line),
        "CAS log at {} did not contain the anytrust-fallback warning \"{fallback_line}\"; \
         this is the only positive proof that the fallback path ran. Full log:\n{cas_logs}",
        cas_log_path.display(),
    );
    for forbidden in [
        "anytrust also requested fallback; using calldata",
        "DA provider requested fallback; using calldata",
    ] {
        assert!(
            !cas_logs.contains(forbidden),
            "CAS log at {} unexpectedly contains the calldata-fallback warning \"{forbidden}\"; \
             the anytrust sidecar was supposed to absorb every store. Full log:\n{cas_logs}",
            cas_log_path.display(),
        );
    }
    println!(
        "Mock primary rejected {store_calls} store calls; CAS logs confirm anytrust-fallback \
         path was taken (calldata_max_size=100 makes calldata fallback infeasible)"
    );

    drop(nitro_node);
    drop(mock);
}

// This base64 data is the raw Espresso transaction payload from a real legacy-format
// transaction on the Espresso Decaf testnet:
// https://explorer.decaf.testnet.espresso.network/transaction/9516848-0
const LEGACY_TX_BASE64: &str = "AAAAAAAAAEF6kNpljmHHp84DpMq843KWZxw79PAwsT77pNpWzluVdlJJTapjujnJaf825FdSceMTSOWKYdZGMV0qlGHNehSHAAAAAAAAF6NHAAAAAAAAAFP4UfhL+EUNlO3rfipGB0Mr0RW63zcDEIMqR4h8g6eWH4RqIG1ioAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAfxAhAExc1CAgtcigwH8QQ==";
const LEGACY_SIGNER_ADDRESS: &str = "0xedEb7E2a4607432Bd115badf370310832A47887C";

#[tokio::test]
async fn test_e2e_legacy_feed_format() {
    let nitro_node = NitroNode::start(NitroNodeConfig {
        no_l2_traffic: true,
    })
    .await;
    println!("Nitro stack started (no L2 traffic)");

    let espresso = EspressoDevNode::connect().await;
    let starting_hotshot_height = espresso
        .client
        .fetch_latest_hotshot_block_height()
        .await
        .expect("failed to fetch latest hotshot block height")
        + 1;
    println!("Espresso dev node ready; starting_hotshot_height = {starting_hotshot_height}");

    let namespace_id = NamespaceId::from(412346u64);
    let legacy_tx_bytes = general_purpose::STANDARD
        .decode(LEGACY_TX_BASE64)
        .expect("decode legacy tx base64");

    let _hash = espresso
        .client
        .submit_transaction(Transaction::new(namespace_id, legacy_tx_bytes))
        .await
        .expect("submit legacy tx");
    println!("Submitted legacy-format transaction to Espresso");

    let target_height = starting_hotshot_height + 5;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if Instant::now() >= deadline {
            panic!("Espresso did not reach height {target_height} in time");
        }
        match espresso.client.fetch_latest_hotshot_block_height().await {
            Ok(h) if h >= target_height => {
                println!(
                    "Espresso at height {h} (target {target_height}); legacy payload finalized"
                );
                break;
            }
            _ => sleep(Duration::from_secs(1)).await,
        }
    }

    let sequencer_inbox = read_sequencer_inbox_address();
    let tee_verifier_address = read_tee_verifier_address();

    let cas_config_path = write_cas_config(
        starting_hotshot_height,
        CasRoute::Calldata,
        &sequencer_inbox,
        tee_verifier_address,
        true,
        None,
        None,
        None,
    )
    .await;

    let raw = std::fs::read_to_string(&cas_config_path)
        .unwrap_or_else(|e| panic!("read CAS config: {e}"));
    let mut config: Value =
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse CAS config: {e}"));
    config["rollup"]["stack"]["legacy_signer_addresses"] = json!([LEGACY_SIGNER_ADDRESS]);
    std::fs::write(
        &cas_config_path,
        serde_json::to_string_pretty(&config).expect("serialize patched config"),
    )
    .unwrap_or_else(|e| panic!("write patched CAS config: {e}"));
    println!(
        "CAS config written to {} with legacy_signer_addresses=[{LEGACY_SIGNER_ADDRESS}]",
        cas_config_path.display()
    );

    let cas_log_path = std::env::temp_dir().join("cas-e2e-legacy-feed-format.log");
    let probe_url = CasRoute::Calldata.rpc_url_local();
    let cas = spawn_cas_with_retries_to_file(&cas_config_path, &probe_url, &cas_log_path).await;
    println!("CAS started (logs at {})", cas_log_path.display());

    sleep(Duration::from_secs(5)).await;

    let _ = std::process::Command::new("docker")
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/nitro"))
        .args(["compose", "up", "-d", "--wait", "tx-generator"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("start tx-generator");
    println!("tx-generator started (V1 traffic flowing)");

    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster(CAS_FEED_URL, CasRoute::Calldata.rpc_url_for_poster());
    println!("Poster started");

    wait_for_batches_on_l1(&l1, from_block, 2, sequencer_inbox).await;
    println!("Observed >=2 batches on L1");

    drop(cas);

    let cas_logs = std::fs::read_to_string(&cas_log_path)
        .unwrap_or_else(|e| panic!("read CAS log {}: {e}", cas_log_path.display()));

    let legacy_marker = "falling back to legacy format";
    assert!(
        cas_logs.contains(legacy_marker),
        "CAS log at {} does not contain the legacy-fallback marker \"{legacy_marker}\"",
        cas_log_path.display(),
    );
    println!("CAS logs confirm legacy fallback path was exercised");

    let legacy_parse_error = "failed to parse legacy hotshot payload";
    assert!(
        !cas_logs.contains(legacy_parse_error),
        "CAS log at {} contains a legacy parse error \"{legacy_parse_error}\"",
        cas_log_path.display(),
    );

    let signer_rejection = "is not in the list of legacy signer addresses";
    assert!(
        !cas_logs.contains(signer_rejection),
        "CAS log at {} contains a legacy signer rejection;",
        cas_log_path.display(),
    );

    println!("All legacy-format E2E assertions passed");

    drop(nitro_node);
}
