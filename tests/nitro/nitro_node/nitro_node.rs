use std::{
    path::Path,
    process::{Command, Stdio},
    sync::OnceLock,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const COMPOSE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/nitro");
const DAS_KEYS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/nitro/das-keys");
const GENERATED_CONFIG_DIR: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/nitro/generated-config");

#[derive(Debug, Clone, Default)]
pub struct NitroNodeConfig {
    pub no_l2_traffic: bool,
}

pub struct NitroNode {
    _config: NitroNodeConfig,
    _lifecycle_permit: Option<OwnedSemaphorePermit>,
}

fn compose_lifecycle_semaphore() -> &'static std::sync::Arc<Semaphore> {
    static SEMAPHORE: OnceLock<std::sync::Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| std::sync::Arc::new(Semaphore::new(2)))
}

fn run_compose_result(args: &[&str]) -> Result<(), String> {
    let status = Command::new("docker")
        .args(args)
        .current_dir(COMPOSE_DIR)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|err| format!("failed to run docker compose {args:?}: {err}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("docker compose failed: {args:?}"))
    }
}

fn run_compose(args: &[&str]) {
    if let Err(err) = run_compose_result(args) {
        panic!("{err}");
    }
}

fn compose_down_status() -> std::io::Result<std::process::ExitStatus> {
    Command::new("docker")
        .args([
            "compose",
            "--profile",
            "poster",
            "--profile",
            "deploy",
            "--profile",
            "anytrust",
            "down",
            "-v",
            "--remove-orphans",
        ])
        .current_dir(COMPOSE_DIR)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
}

impl NitroNode {
    pub async fn start(config: NitroNodeConfig) -> Self {
        let lifecycle_permit = compose_lifecycle_semaphore()
            .clone()
            .acquire_owned()
            .await
            .expect("lifecycle semaphore closed");

        let _ = compose_down_status();

        let startup = (|| -> Result<(), String> {
            println!("Fresh deployment — deploying rollup contracts to L1");

            run_compose_result(&[
                "compose",
                "up",
                "-d",
                "--wait",
                "l1-anvil",
                "espresso-dev-node",
            ])?;

            run_compose_result(&["compose", "--profile", "deploy", "up", "rollup-creator"])?;

            let mut args: Vec<&str> = vec!["compose", "up", "-d", "--wait", "sequencer"];
            if !config.no_l2_traffic {
                args.push("tx-generator");
            }
            run_compose_result(&args)
        })();

        if let Err(err) = startup {
            let _ = compose_down_status();
            panic!("{err}");
        }

        println!("Nitro stack ready (L1 + sequencer)");

        Self {
            _config: config,
            _lifecycle_permit: Some(lifecycle_permit),
        }
    }

    pub fn start_poster(&self, cas_feed_url: &str, cas_calldata_rpc_url: &str) {
        let _ = Command::new("docker")
            .args([
                "compose",
                "--profile",
                "poster",
                "rm",
                "-f",
                "-s",
                "-v",
                "poster",
            ])
            .current_dir(COMPOSE_DIR)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status();

        let status = Command::new("docker")
            .args([
                "compose",
                "--profile",
                "poster",
                "up",
                "-d",
                "--wait",
                "poster",
            ])
            .env("CAS_FEED_URL", cas_feed_url)
            .env("CAS_CALLDATA_RPC_URL", cas_calldata_rpc_url)
            .current_dir(COMPOSE_DIR)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .expect("failed to run `docker compose up --wait poster`");
        assert!(status.success(), "`docker compose up --wait poster` failed");
    }

    pub fn start_das_committee(&self) {
        run_compose(&[
            "compose",
            "--profile",
            "anytrust",
            "up",
            "-d",
            "--wait",
            "das-committee-a",
            "das-committee-b",
            "das-mirror",
        ]);
    }

    pub fn start_anytrust_daprovider(&self, sequencer_inbox_address: &str) {
        let status = Command::new("docker")
            .args([
                "compose",
                "--profile",
                "anytrust",
                "up",
                "-d",
                "--wait",
                "daprovider-anytrust",
            ])
            .env("SEQUENCER_INBOX_ADDRESS", sequencer_inbox_address)
            .current_dir(COMPOSE_DIR)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .expect("failed to run `docker compose up --wait daprovider-anytrust`");
        assert!(
            status.success(),
            "`docker compose up --wait daprovider-anytrust` failed"
        );
    }

    /// Registers the DAS committee keyset on the L1 SequencerInbox so that AnyTrust
    /// batches are accepted. Uses `anytrusttool dumpkeyset` to produce the exact
    /// serialized keyset that the daprovider will embed in DAS certificates.
    pub fn register_das_keyset(&self, private_key: &str) {
        let key_a = std::fs::read_to_string(Path::new(DAS_KEYS_DIR).join("a/das_bls.pub"))
            .expect("read das_bls.pub for committee-a");
        let key_b = std::fs::read_to_string(Path::new(DAS_KEYS_DIR).join("b/das_bls.pub"))
            .expect("read das_bls.pub for committee-b");

        let config = serde_json::json!({
            "keyset": {
                "assumed-honest": 1,
                "backends": [
                    {"url": "http://das-committee-a:9876", "pubkey": key_a.trim()},
                    {"url": "http://das-committee-b:9876", "pubkey": key_b.trim()},
                ]
            }
        });
        let config_path = std::env::temp_dir().join("das-dumpkeyset.json");
        std::fs::write(&config_path, config.to_string()).expect("write dumpkeyset config");

        let dump_output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--entrypoint",
                "/usr/local/bin/anytrusttool",
                "-v",
                &format!("{}:/config.json:ro", config_path.display()),
                "offchainlabs/nitro-node:v3.10.0-b1cf6db",
                "dumpkeyset",
                "--conf.file",
                "/config.json",
            ])
            .output()
            .expect("failed to run anytrusttool dumpkeyset");
        assert!(
            dump_output.status.success(),
            "anytrusttool dumpkeyset failed: {}",
            String::from_utf8_lossy(&dump_output.stderr)
        );
        let stdout = String::from_utf8(dump_output.stdout).expect("invalid utf8");
        let keyset_hex = stdout
            .lines()
            .find(|l| l.starts_with("Keyset: "))
            .expect("missing Keyset line in dumpkeyset output")
            .strip_prefix("Keyset: ")
            .expect("strip prefix")
            .trim()
            .to_string();

        let deployment: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(Path::new(GENERATED_CONFIG_DIR).join("deployment.json"))
                .expect("read deployment.json"),
        )
        .expect("parse deployment.json");
        let sequencer_inbox = deployment["sequencer-inbox"]
            .as_str()
            .expect("missing sequencer-inbox");
        let upgrade_executor = deployment["upgrade-executor"]
            .as_str()
            .expect("missing upgrade-executor");

        let inner_calldata_output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--entrypoint",
                "cast",
                "ghcr.io/foundry-rs/foundry:latest",
                "calldata",
                "setValidKeyset(bytes)",
                &keyset_hex,
            ])
            .output()
            .expect("failed to encode setValidKeyset calldata");
        assert!(
            inner_calldata_output.status.success(),
            "cast calldata for setValidKeyset failed"
        );
        let inner_calldata = String::from_utf8(inner_calldata_output.stdout)
            .expect("invalid utf8")
            .trim()
            .to_string();

        let status = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "nitro_default",
                "--entrypoint",
                "cast",
                "ghcr.io/foundry-rs/foundry:latest",
                "send",
                "--rpc-url",
                "http://l1-anvil:8545",
                "--private-key",
                private_key,
                upgrade_executor,
                "executeCall(address,bytes)",
                sequencer_inbox,
                &inner_calldata,
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .expect("failed to run cast send for keyset registration");
        assert!(
            status.success(),
            "DAS keyset registration failed (cast send exited with {status})"
        );
        println!("DAS keyset registered on SequencerInbox {sequencer_inbox}");
    }

    pub fn stop(&self) {
        let status = compose_down_status();

        match status {
            Ok(s) if s.success() => {}
            Ok(s) => {
                let msg = format!("`docker compose down -v` exited with status: {s}");
                if std::thread::panicking() {
                    eprintln!("ERROR during cleanup: {msg}");
                } else {
                    panic!("{}", msg);
                }
            }
            Err(err) => {
                let msg = format!("`docker compose down -v` failed to execute: {err}");
                if std::thread::panicking() {
                    eprintln!("ERROR during cleanup: {msg}");
                } else {
                    panic!("{}", msg);
                }
            }
        }
    }
}

impl Drop for NitroNode {
    fn drop(&mut self) {
        self.stop();
    }
}
