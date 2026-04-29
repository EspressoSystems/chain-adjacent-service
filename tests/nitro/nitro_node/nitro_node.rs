use reqwest::Url;
use serde::de;
use serde_json::json;
use std::result::Result::Ok;
use std::{
    process::{Child, Command, Stdio},
    sync::{Mutex, OnceLock},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, sleep};

const REFERENCE_DA_ENDPOINT: i32 = 9880;
const TESTNODE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/nitro-testnode");

#[derive(Debug, Clone, Default)]
pub struct NitroNodeConfig {
    pub reference_da_url: Option<Url>,
    // If true, the node will be started with `--simple`,
    // and all components will run in a single container.
    pub simple: bool,
    // if true, will set up a validator
    pub validator: bool,
    pub cas_feed_url: Option<Url>,
    // Used to check if the sequencer is ready
    pub sequencer_url: Option<Url>,
}

impl NitroNodeConfig {
    pub fn with_reference_da(mut self, url: Url) -> Self {
        self.reference_da_url = Some(url);
        self
    }

    pub fn with_cas_feed(mut self, url: Url) -> Self {
        self.cas_feed_url = Some(url);
        self
    }

    pub fn with_simple(mut self, simple: bool) -> Self {
        self.simple = simple;
        self
    }

    pub fn with_validator(mut self, validator: bool) -> Self {
        self.validator = validator;
        self
    }

    pub fn default_reference_da() -> Self {
        let url = format!("http://localhost:{REFERENCE_DA_ENDPOINT}")
            .parse()
            .expect("valid URL");
        Self {
            reference_da_url: Some(url),
            ..Default::default()
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.simple && self.cas_feed_url.is_some() {
            anyhow::bail!("CAS feed is not supported in simple mode");
        }
        Ok(())
    }
}

/// NitroNode runs a local Arbitrum Nitro node
/// for testing purposes.
pub struct NitroNode {
    pub config: NitroNodeConfig,
    // Wrapped in Mutex because Child requires &mut to kill,
    // but stop() only has &self
    process: Mutex<Child>,
    _lifecycle_permit: Option<OwnedSemaphorePermit>,
}

fn compose_lifecycle_semaphore() -> &'static std::sync::Arc<Semaphore> {
    static SEMAPHORE: OnceLock<std::sync::Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| std::sync::Arc::new(Semaphore::new(2)))
}

impl NitroNode {
    pub async fn start(config: NitroNodeConfig) -> Self {
        let lifecycle_permit = compose_lifecycle_semaphore()
            .clone()
            .acquire_owned()
            .await
            .expect("lifecycle semaphore closed");

        let script = std::path::Path::new(TESTNODE_DIR).join("test-node.bash");

        assert!(
            script.exists(),
            "nitro-testnode submodule not initialized. Run: git submodule update --init --recursive"
        );

        let mut args = vec![
            "--init-force",
            "--no-tokenbridge",
            "--no-run",
            "--no-l2-traffic",
            "--detach",
        ];

        if !config.simple {
            args.push("--no-simple");
        }

        if config.reference_da_url.is_some() {
            args.push("--l2-referenceda");
        }

        if config.validator {
            args.push("--validate");
        }

        // if config.l1_only {
        //     args.push("--no-run");
        // }

        let child = Command::new("bash")
            .arg("./test-node.bash")
            .args(&args)
            // critical: script resolves all its relative paths from its own repo root
            .current_dir(TESTNODE_DIR)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect(
                "failed to spawn test-node.bash — is the nitro-testnode submodule initialized?\n\
                 Run: git submodule update --init --recursive",
            );

        let node = Self {
            config,
            process: Mutex::new(child),
            _lifecycle_permit: Some(lifecycle_permit),
        };

        node.wait_until_ready().await;
        node
    }

    /// Polls `getSupportedBytes` until the node sends a 200 Ok response
    async fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(600);
        println!("Waiting for node to be ready...");

        let mut da_ready = self.config.reference_da_url.is_none();
        let mut sequencer_ready = false;
        loop {
            if Instant::now() >= deadline {
                self.stop();
                panic!("timed out waiting for node to be ready");
            }

            if let Some(da_url) = &self.config.reference_da_url {
                if !da_ready {
                    match fetch_da_provider_byte(da_url).await {
                        Ok(_) => {
                            da_ready = true;
                            println!("DA provider is ready");
                        }
                        Err(err) => {
                            println!("DA provider not ready: {err}");
                        }
                    }
                }
            } else {
                da_ready = true;
            }
            // check if sequencer is ready
            if !sequencer_ready && self.config.sequencer_url.is_some() {
                let sequencer_url = self.config.sequencer_url.as_ref().unwrap();
                match fetch_block_height(sequencer_url).await {
                    Ok(_) => sequencer_ready = true,
                    Err(_) => {
                        println!("waiting for sequencer to be ready")
                    }
                }
            }

            if da_ready && sequencer_ready {
                println!("Node is ready!");
                break;
            }

            println!("sequencer: {sequencer_ready}, da: {da_ready}");
            sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn stop(&self) {
        // Kill and reap the bash process first
        if let Ok(mut child) = self.process.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }

        // docker compose down -v: remove containers and volumes
        let status = Command::new("docker")
            .args(["compose", "down", "-v"])
            .current_dir(TESTNODE_DIR)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status();

        match status {
            Ok(s) if s.success() => {}
            Ok(s) => {
                let msg = format!("`docker compose down -v` exited with status: {s}");
                // If we're already panicking, a second panic would abort the
                // process immediately, swallowing the original panic message.
                // Print instead and let the original panic surface cleanly.
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
        // self.stop();
    }
}

pub async fn fetch_da_provider_byte(url: &Url) -> anyhow::Result<()> {
    let request_body = json!({
        "jsonrpc": "2.0",
        "method": "daprovider_getSupportedHeaderBytes",
        "params": [],
        "id": 1
    });
    let client = reqwest::Client::new();

    let response = client
        .post(url.clone())
        .json(&request_body)
        .send()
        .await
        .map_err(|err| anyhow::anyhow!("Connection failed: {err}"))?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Service returned status: {}",
            response.status()
        ));
    }

    let body: serde_json::Value = response.json().await?;
    if body.get("error").is_some() {
        return Err(anyhow::anyhow!("DA Provider returned a JSON-RPC error"));
    }

    Ok(())
}

pub async fn fetch_block_height(url: &Url) -> anyhow::Result<u64> {
    let request_body = json!({
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    });
    let client = reqwest::Client::new();

    let response = client
        .post(url.clone())
        .json(&request_body)
        .send()
        .await
        .map_err(|err| anyhow::anyhow!("Connection failed: {err}"))?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Service returned status: {}",
            response.status()
        ));
    }

    let body: serde_json::Value = response.json().await?;
    if let Some(error) = body.get("error") {
        return Err(anyhow::anyhow!("JSON-RPC error: {error}"));
    }

    let block_number_hex = body
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing or invalid result field"))?;

    let block_number = u64::from_str_radix(&block_number_hex.trim_start_matches("0x"), 16)
        .map_err(|err| anyhow::anyhow!("Failed to parse block number: {err}"))?;

    Ok(block_number)
}
