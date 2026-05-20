use std::{
    process::{Command, Stdio},
    sync::OnceLock,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const COMPOSE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/nitro");

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

    pub fn stop_poster(&self) {
        run_compose(&[
            "compose",
            "--profile",
            "poster",
            "stop",
            "poster",
        ]);
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
