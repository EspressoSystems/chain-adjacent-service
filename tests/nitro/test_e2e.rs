use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::{Instant, sleep};

use chain_agnostic_service::espresso_e2e::espresso_dev_node::EspressoDevNode;

use crate::nitro_node::nitro_node::{NitroNode, NitroNodeConfig};

const CAS_BIN: &str = env!("CARGO_BIN_EXE_chain-agnostic-service");
const WRITE_OVERRIDE_SCRIPT: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/nitro/write-override.sh");

const CAS_FEED_URL: &str = "ws://host.docker.internal:9643";
const CAS_CALLDATA_RPC_URL: &str = "http://host.docker.internal:8000/cas/arb/calldata";
/// Same endpoint as `CAS_CALLDATA_RPC_URL` but reachable from the host
/// (the `host.docker.internal` form is only meaningful inside the testnode
/// containers).
const CAS_CALLDATA_RPC_URL_LOCAL: &str = "http://localhost:8000/cas/arb/calldata";

/// RAII wrapper that kills the CAS subprocess on drop so the test never
/// leaks a background process if it panics.
struct CasProcess(Child);

impl Drop for CasProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Writes `nitro-testnode/docker-compose.override.yml` so that:
///   - geth is replaced by our anvil L1 image, which loads the persisted
///     state from `tests/nitro/l1_node/state/anvil-state.json` on startup.
///   - rollupcreator short-circuits deployment by copying the saved
///     artifacts from the same directory.
fn setup_l1_reuse_mode() {
    let status = Command::new("bash")
        .arg(WRITE_OVERRIDE_SCRIPT)
        .arg("reuse")
        .env("CAS_CALLDATA_RPC_URL", CAS_CALLDATA_RPC_URL)
        .env("CAS_FEED_URL", CAS_FEED_URL)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("failed to run write-override.sh");
    assert!(status.success(), "write-override.sh reuse failed");
}

fn spawn_cas(config_path: &Path) -> CasProcess {
    let child = Command::new(CAS_BIN)
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn CAS binary");
    CasProcess(child)
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

/// Polls CAS's calldata DA RPC until it answers `daprovider_getSupportedHeaderBytes`.
/// This is what the poster will call once it comes up, so it's the right
/// readiness signal before transitioning to the next phase.
async fn wait_for_cas_ready() {
    let deadline = Instant::now() + Duration::from_secs(120);
    let client = reqwest::Client::new();
    let body = json!({
        "jsonrpc": "2.0",
        "method": "daprovider_getSupportedHeaderBytes",
        "params": [],
        "id": 1,
    });

    loop {
        if Instant::now() >= deadline {
            panic!("timed out waiting for CAS DA RPC at {CAS_CALLDATA_RPC_URL_LOCAL}");
        }

        match client
            .post(CAS_CALLDATA_RPC_URL_LOCAL)
            .json(&body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                println!("CAS DA RPC is ready");
                return;
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

#[tokio::test]
async fn test_e2e() {
    let espresso = EspressoDevNode::start().await;
    println!(
        "Espresso dev node started at {}",
        espresso.client.config.base_url
    );

    setup_l1_reuse_mode();

    let config = NitroNodeConfig {
        sequencer_url: Some("http://localhost:8547".parse().unwrap()),
        no_l2_traffic: true,
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
    let cas = spawn_cas(&cas_config_path);
    wait_for_cas_ready().await;

    // Phase 3: bring up the poster. Its overridden command (set by
    // write-override.sh) points at CAS for the feed and DA RPC.
    nitro_node.start_poster();
    println!("Poster started");

    // TODO: real e2e assertions (e.g. submit a tx on L2, confirm a batch
    // flows through CAS, verify the espresso-wrapped DA cert on L1).
    sleep(Duration::from_secs(5 * 60)).await;

    // Explicit drop order: poster goes down with nitro_node.stop(), then
    // CAS, then espresso — downstream consumers tear down before the
    // things they depend on.
    drop(cas);
    drop(nitro_node);
    drop(espresso);
}
