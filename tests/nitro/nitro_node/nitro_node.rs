use std::{
    process::{Command, Stdio},
    sync::OnceLock,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const COMPOSE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/nitro");
const UNUSED_CAS_FEED_URL: &str = "ws://unused.invalid";
const UNUSED_CAS_CALLDATA_RPC_URL: &str = "http://unused.invalid";
const UNUSED_SEQUENCER_INBOX_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

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

fn compose_command() -> Command {
    let mut command = Command::new("docker");
    command
        .current_dir(COMPOSE_DIR)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env("CAS_FEED_URL", UNUSED_CAS_FEED_URL)
        .env("CAS_CALLDATA_RPC_URL", UNUSED_CAS_CALLDATA_RPC_URL)
        .env("SEQUENCER_INBOX_ADDRESS", UNUSED_SEQUENCER_INBOX_ADDRESS);
    command
}

fn run_compose_result(args: &[&str]) -> Result<(), String> {
    let status = compose_command()
        .args(args)
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
    compose_command()
        .args([
            "compose",
            "--profile",
            "poster",
            "--profile",
            "deploy",
            "--profile",
            "anytrust",
            "--profile",
            "validator",
            "down",
            "-v",
            "--remove-orphans",
        ])
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
            println!("Loading pre-deployed L1 state (run `just generate-l1-state` to regenerate)");

            run_compose_result(&[
                "compose",
                "up",
                "-d",
                "--wait",
                "l1-anvil",
                "espresso-dev-node",
            ])?;

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

    pub fn start_poster(
        &self,
        cas_feed_url: &str,
        cas_calldata_rpc_url: &str,
        sequencer_inbox_address: &str,
    ) {
        let _ = compose_command()
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
            .status();

        let status = compose_command()
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
            .env("SEQUENCER_INBOX_ADDRESS", sequencer_inbox_address)
            .status()
            .expect("failed to run `docker compose up --wait poster`");
        assert!(status.success(), "`docker compose up --wait poster` failed");
    }

    pub fn start_das_committee(&self, sequencer_inbox_address: &str) {
        let status = compose_command()
            .args([
                "compose",
                "--profile",
                "anytrust",
                "up",
                "-d",
                "--wait",
                "das-committee-a",
                "das-committee-b",
                "das-mirror",
            ])
            .env("SEQUENCER_INBOX_ADDRESS", sequencer_inbox_address)
            .status()
            .expect("failed to run `docker compose up --wait das-committee`");
        assert!(
            status.success(),
            "`docker compose up --wait das-committee` failed"
        );
    }

    pub fn start_anytrust_daprovider(&self, sequencer_inbox_address: &str) {
        let status = compose_command()
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
            .status()
            .expect("failed to run `docker compose up --wait daprovider-anytrust`");
        assert!(
            status.success(),
            "`docker compose up --wait daprovider-anytrust` failed"
        );
    }

    pub fn stop_poster(&self) {
        run_compose(&["compose", "--profile", "poster", "stop", "poster"]);
    }

    pub fn start_block_validator(&self) {
        run_compose(&[
            "compose",
            "--profile",
            "validator",
            "up",
            "-d",
            "--wait",
            "validator",
        ]);
    }

    pub fn pause_espresso_dev_node(&self) {
        run_compose(&["compose", "pause", "espresso-dev-node"]);
    }

    pub fn unpause_espresso_dev_node(&self) {
        run_compose(&["compose", "unpause", "espresso-dev-node"]);
    }

    pub fn stop_tx_generator(&self) {
        run_compose(&["compose", "stop", "tx-generator"]);
    }

    pub fn stop(&self) {
        let _ = run_compose_result(&["compose", "unpause", "espresso-dev-node"]);
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
