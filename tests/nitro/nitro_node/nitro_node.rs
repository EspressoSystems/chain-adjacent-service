use std::{
    fs,
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

enum DeploymentCacheStatus {
    Missing,
    Valid,
    Invalid(String),
}

fn parse_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(num) => num.as_u64(),
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if let Some(hex) = trimmed
                .strip_prefix("0x")
                .or_else(|| trimmed.strip_prefix("0X"))
            {
                u64::from_str_radix(hex, 16).ok()
            } else {
                trimmed.parse().ok()
            }
        }
        _ => None,
    }
}

fn deployment_cache_status() -> DeploymentCacheStatus {
    let dir = Path::new(GENERATED_CONFIG_DIR);
    let l1_state_path = dir.join("l1-state.json");
    let deployment_path = dir.join("deployment.json");
    let chain_info_path = dir.join("deployed_chain_info.json");

    if !l1_state_path.exists() || !deployment_path.exists() || !chain_info_path.exists() {
        return DeploymentCacheStatus::Missing;
    }

    let l1_state: serde_json::Value = match fs::read_to_string(&l1_state_path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
    {
        Some(value) => value,
        None => {
            return DeploymentCacheStatus::Invalid(format!(
                "failed to parse {}",
                l1_state_path.display()
            ));
        }
    };
    let deployment: serde_json::Value = match fs::read_to_string(&deployment_path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
    {
        Some(value) => value,
        None => {
            return DeploymentCacheStatus::Invalid(format!(
                "failed to parse {}",
                deployment_path.display()
            ));
        }
    };

    let l1_block = match l1_state.get("block").and_then(|block| block.get("number")) {
        Some(value) => match parse_u64(value) {
            Some(block) => block,
            None => {
                return DeploymentCacheStatus::Invalid(format!(
                    "invalid block number in {}",
                    l1_state_path.display()
                ));
            }
        },
        None => {
            return DeploymentCacheStatus::Invalid(format!(
                "missing block.number in {}",
                l1_state_path.display()
            ));
        }
    };
    let deployed_at = match deployment.get("deployed-at") {
        Some(value) => match parse_u64(value) {
            Some(block) => block,
            None => {
                return DeploymentCacheStatus::Invalid(format!(
                    "invalid deployed-at in {}",
                    deployment_path.display()
                ));
            }
        },
        None => {
            return DeploymentCacheStatus::Invalid(format!(
                "missing deployed-at in {}",
                deployment_path.display()
            ));
        }
    };

    if l1_block < deployed_at {
        return DeploymentCacheStatus::Invalid(format!(
            "cached L1 state only reaches block {l1_block}, but deployment metadata needs block {deployed_at}"
        ));
    }

    DeploymentCacheStatus::Valid
}

fn clear_generated_config_cache() {
    for file in [
        "l1-state.json",
        "deployment.json",
        "deployed_chain_info.json",
        "tee_verifier_address.txt",
    ] {
        let path = Path::new(GENERATED_CONFIG_DIR).join(file);
        if let Err(err) = fs::remove_file(&path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                panic!("failed to remove {}: {err}", path.display());
            }
        }
    }
}

fn run_compose_result(args: &[&str]) -> Result<(), String> {
    let status = Command::new("docker")
        .args(args)
        .current_dir(COMPOSE_DIR)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|err| format!("failed to run docker compose {:?}: {err}", args))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("docker compose failed: {:?}", args))
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

        let cached = match deployment_cache_status() {
            DeploymentCacheStatus::Missing => false,
            DeploymentCacheStatus::Valid => true,
            DeploymentCacheStatus::Invalid(reason) => {
                println!("Discarding stale Nitro deployment cache: {reason}");
                clear_generated_config_cache();
                false
            }
        };

        let startup = (|| -> Result<(), String> {
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
                run_compose_result(&args)
            } else {
                println!("Fresh deployment (L1 state will be cached when Anvil stops)");

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
            }
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
