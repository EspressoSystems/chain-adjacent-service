use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;

use chain_agnostic_service::espresso_e2e::espresso_dev_node::EspressoDevNode;

use crate::nitro_node::nitro_node::{NitroNode, NitroNodeConfig};

const CAS_BIN: &str = env!("CARGO_BIN_EXE_chain-agnostic-service");
const CAS_CONFIG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/nitro/cas-config.json");
const WRITE_OVERRIDE_SCRIPT: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/nitro/write-override.sh");

const CAS_FEED_URL: &str = "http://localhost:9643";
const CAS_CALLDATA_RPC_URL: &str = "http://localhost:8000/arb/calldata";

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

fn spawn_cas() -> CasProcess {
    let child = Command::new(CAS_BIN)
        .arg("--config")
        .arg(CAS_CONFIG_PATH)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn CAS binary");
    CasProcess(child)
}

#[tokio::test]
async fn test_e2e() {
    let espresso = EspressoDevNode::start().await;
    println!("Espresso dev node started at {}", espresso.client.config.base_url);

    setup_l1_reuse_mode();

    let nitro_node = NitroNode::start(NitroNodeConfig::default()).await;
    println!("Nitro node + L1 (reuse mode) started");

    let _cas = spawn_cas();
    println!("CAS spawned with config {CAS_CONFIG_PATH}");

    sleep(Duration::from_secs(5)).await;

    // TODO: real e2e assertions (e.g. submit a tx on L2, confirm a batch
    // flows through CAS, verify the espresso-wrapped DA cert on L1).

    // Explicit drop order: nitro first, then CAS, then espresso — so the
    // downstream consumers tear down before the things they depend on.
    drop(nitro_node);
    drop(_cas);
    drop(espresso);
}
