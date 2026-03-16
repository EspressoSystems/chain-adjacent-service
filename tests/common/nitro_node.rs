use reqwest::Url;
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

pub struct NitroNodeConfig {
    pub reference_da_url: Url,
}

impl NitroNodeConfig {
    pub async fn fetch_da_provider_byte(&self) -> anyhow::Result<()> {
        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_getSupportedHeaderBytes",
            "params": [],
            "id": 1
        });
        let client = reqwest::Client::new();

        let response = client
            .post(self.reference_da_url.clone())
            .json(&request_body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Connection failed: {}", e))?;

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
}

/// NitroNode runs a local Arbitrum Nitro node
/// for testing purposes.
pub struct NitroNode {
    pub client: NitroNodeConfig,
    // Wrapped in Mutex because Child requires &mut to kill,
    // but stop() only has &self
    process: Mutex<Child>,
    _lifecycle_permit: Option<OwnedSemaphorePermit>,
}

fn compose_lifecycle_semaphore() -> &'static std::sync::Arc<Semaphore> {
    static SEMAPHORE: OnceLock<std::sync::Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| std::sync::Arc::new(Semaphore::new(1)))
}

impl NitroNode {
    pub async fn start() -> Self {
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

        let child = Command::new("bash")
            .arg("./test-node.bash")
            .args(["--init", "--l2-referenceda", "--simple"])
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
            client: NitroNodeConfig {
                reference_da_url: Url::parse(&format!(
                    "http://localhost:{}",
                    REFERENCE_DA_ENDPOINT
                ))
                .unwrap(),
            },
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
        loop {
            if Instant::now() >= deadline {
                self.stop();
                panic!("timed out waiting for node to be ready");
            }
            match self.client.fetch_da_provider_byte().await {
                Ok(_) => break,
                _ => sleep(Duration::from_millis(100)).await,
            }
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
                let msg = format!("`docker compose down -v` exited with status: {}", s);
                // If we're already panicking, a second panic would abort the
                // process immediately, swallowing the original panic message.
                // Print instead and let the original panic surface cleanly.
                if std::thread::panicking() {
                    eprintln!("ERROR during cleanup: {}", msg);
                } else {
                    panic!("{}", msg);
                }
            }
            Err(e) => {
                let msg = format!("`docker compose down -v` failed to execute: {}", e);
                if std::thread::panicking() {
                    eprintln!("ERROR during cleanup: {}", msg);
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
