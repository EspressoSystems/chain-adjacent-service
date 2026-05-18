use std::{
    path::Path,
    process::{Command, Stdio},
    sync::OnceLock,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const COMPOSE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/nitro");
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

fn is_deployment_cached() -> bool {
    let dir = Path::new(GENERATED_CONFIG_DIR);
    dir.join("l1-state.json").exists()
        && dir.join("deployment.json").exists()
        && dir.join("deployed_chain_info.json").exists()
}

fn run_compose(args: &[&str]) {
    let status = Command::new("docker")
        .args(args)
        .current_dir(COMPOSE_DIR)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("failed to run docker compose");
    assert!(status.success(), "docker compose failed: {:?}", args);
}

fn dump_l1_state() {
    let state_path = Path::new(GENERATED_CONFIG_DIR).join("l1-state.json");

    // Stop anvil gracefully — --dump-state writes l1-state.json on exit.
    run_compose(&["compose", "stop", "l1-anvil"]);
    assert!(
        state_path.exists(),
        "anvil did not dump state to {}",
        state_path.display()
    );
    println!("L1 state cached to {}", state_path.display());

    // Restart anvil — entrypoint detects l1-state.json and uses --load-state.
    run_compose(&["compose", "up", "-d", "--wait", "l1-anvil"]);
}

impl NitroNode {
    pub async fn start(config: NitroNodeConfig) -> Self {
        let lifecycle_permit = compose_lifecycle_semaphore()
            .clone()
            .acquire_owned()
            .await
            .expect("lifecycle semaphore closed");

        let _ = Command::new("docker")
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
            .status();

        let cached = is_deployment_cached();

        if cached {
            println!("Using cached L1 deployment (skipping rollup-creator)");
            let mut args: Vec<&str> = vec![
                "compose",
                "up",
                "-d",
                "--wait",
                "l1-anvil",
                "espresso-dev-node",
                "sequencer",
            ];
            if !config.no_l2_traffic {
                args.push("tx-generator");
            }
            run_compose(&args);
        } else {
            println!("Fresh deployment (will cache L1 state after)");

            run_compose(&[
                "compose",
                "up",
                "-d",
                "--wait",
                "l1-anvil",
                "espresso-dev-node",
            ]);

            run_compose(&["compose", "--profile", "deploy", "up", "rollup-creator"]);

            dump_l1_state();

            let mut args: Vec<&str> = vec!["compose", "up", "-d", "--wait", "sequencer"];
            if !config.no_l2_traffic {
                args.push("tx-generator");
            }
            run_compose(&args);
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

    pub fn stop(&self) {
        let status = Command::new("docker")
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
            .status();

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
