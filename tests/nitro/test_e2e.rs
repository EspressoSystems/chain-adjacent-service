use alloy::primitives::{Address, address};
use alloy::providers::{Provider, RootProvider};
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::{Instant, sleep};

use chain_agnostic_service::espresso_e2e::espresso_dev_node::EspressoDevNode;
use chain_agnostic_service::rollups::nitro::l1_monitor::ISequencerInbox;

use crate::cas_harness::setup_l1_reuse_mode_with_cas_poster;
use crate::nitro_node::nitro_node::{NitroNode, NitroNodeConfig};

const CAS_BIN: &str = env!("CARGO_BIN_EXE_chain-agnostic-service");

const CAS_FEED_URL: &str = "ws://host.docker.internal:9643";
const CAS_CALLDATA_RPC_URL: &str = "http://host.docker.internal:8000/cas/arb/calldata";
/// Same endpoint as `CAS_CALLDATA_RPC_URL` but reachable from the host
/// (the `host.docker.internal` form is only meaningful inside the testnode
/// containers).
const CAS_CALLDATA_RPC_URL_LOCAL: &str = "http://localhost:8000/cas/arb/calldata";

const L1_WS_URL: &str = "ws://localhost:8546";
const SEQUENCER_INBOX: Address = address!("18d19C5d3E685f5be5b9C86E097f0E439285D216");

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

/// Spawns CAS and waits for it to become ready, retrying the whole
/// (spawn -> wait) cycle if CAS exits early or doesn't come up in time.
///
/// CAS startup hits the L1 WS endpoint to fetch the latest checkpoint,
/// and that connection occasionally fails with broken-pipe / network-down
/// from anvil — which currently causes CAS to exit. Retrying side-steps
/// the flake until we add proper retry logic inside CAS itself.
async fn spawn_cas_with_retries(config_path: &Path) -> CasProcess {
    const MAX_ATTEMPTS: usize = 5;
    const PER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);

    for attempt in 1..=MAX_ATTEMPTS {
        println!("CAS spawn attempt {attempt}/{MAX_ATTEMPTS}");
        let mut cas = spawn_cas(config_path);

        match tokio::time::timeout(PER_ATTEMPT_TIMEOUT, wait_for_cas_ready(&mut cas)).await {
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

/// Polls CAS's calldata DA RPC until it answers `daprovider_getSupportedHeaderBytes`,
/// or returns `Err` if the subprocess exits before that happens. The
/// caller is expected to apply a timeout via `tokio::time::timeout`.
async fn wait_for_cas_ready(cas: &mut CasProcess) -> Result<(), String> {
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

        match client
            .post(CAS_CALLDATA_RPC_URL_LOCAL)
            .json(&body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                println!("CAS DA RPC is ready");
                return Ok(());
            }
            Ok(resp) => {
                println!("waiting for CAS: status {}", resp.status());
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

/// Builds the CAS config inline and writes it to a runtime path so each
/// test can inject the right `streamer.starting_hotshot_height` (the
/// Espresso dev node container is reused across runs and accumulates
/// hotshot blocks; without this, CAS would replay stale messages from a
/// prior test against a freshly reset L1).
///
/// Sets `is_fresh_deployment: true` so `resolve_config_with_checkpoint`
/// preserves the `starting_hotshot_height` we wrote here instead of
/// overwriting it with whatever it scans off L1.
fn write_cas_config(starting_hotshot_height: u64) -> PathBuf {
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
                "sequencer_inbox_address": "0x18d19C5d3E685f5be5b9C86E097f0E439285D216"
            }
        },
        "da_server": {
            "listen_addr": "0.0.0.0:8000"
        },
        "submitter": {
            "max_in_flight": 1000
        },
        "key_manager": {
            "rpc_url": "http://localhost:8545",
            "tee_verifier_address": "0x0000000000000000000000000000000000000000",
            "attestation_verifier_url": "http://localhost:9000"
        },
        "is_fresh_deployment": true,
    });

    let path = std::env::temp_dir().join("cas-config-e2e.json");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&config).expect("serialize CAS config"),
    )
    .expect("failed to write CAS config");
    path
}

/// Polls L1 for `SequencerBatchDelivered` events on the SequencerInbox
/// starting from `from_block`, returning once at least `min` events have
/// been observed. Snapshotting `from_block` before the poster starts
/// makes the assertion robust against any pre-existing events in the
/// loaded anvil snapshot.
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

#[tokio::test]
async fn test_e2e() {
    let espresso = EspressoDevNode::start().await;
    println!(
        "Espresso dev node started at {}",
        espresso.client.config.base_url
    );

    setup_l1_reuse_mode_with_cas_poster(CAS_FEED_URL, CAS_CALLDATA_RPC_URL);

    let config = NitroNodeConfig {
        // L2 traffic generator is required: it produces the L2 txs that
        // the sequencer batches and the poster eventually posts on L1 —
        // without it there'd be nothing to observe.
        no_l2_traffic: false,
        ..Default::default()
    };

    // Phase 1: bring up L1 + sequencer (no poster yet — it's gated behind
    // CAS being reachable on the feed and DA endpoints).
    let nitro_node = NitroNode::start(config).await;
    println!("Nitro node + L1 (reuse mode) started");

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
    let cas_config_path = write_cas_config(starting_hotshot_height);
    println!(
        "CAS config written to {} (starting_hotshot_height={starting_hotshot_height})",
        cas_config_path.display()
    );
    let cas = spawn_cas_with_retries(&cas_config_path).await;

    // Phase 3: snapshot the L1 head so the batch-count assertion ignores
    // anything pre-existing in the anvil snapshot, then bring up the
    // poster. Its overridden command (set by write-override.sh) points
    // at CAS for the feed and DA RPC.
    let l1 = connect_l1_ws_with_retries().await;
    let from_block = l1
        .get_block_number()
        .await
        .expect("failed to read L1 head block number");

    nitro_node.start_poster();
    println!("Poster started");

    // Phase 4: assert the end-to-end pipeline produces at least 5 batches
    // on L1 (poster -> CAS feed -> Espresso -> CAS DA -> poster -> L1).
    wait_for_batches_on_l1(&l1, from_block, 5).await;

    // Explicit drop order: poster goes down with nitro_node.stop(), then
    // CAS, then espresso — downstream consumers tear down before the
    // things they depend on.
    drop(cas);
    drop(nitro_node);
    drop(espresso);
}
