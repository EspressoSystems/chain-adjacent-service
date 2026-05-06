use crate::espresso_client::client::EspressoClient;
use std::{process::Command, sync::OnceLock, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, sleep};

const ESPRESSO_SEQUENCER_API_PORT: i32 = 41000;
const COMPOSE_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/espresso_e2e/docker-compose.yml"
);

/// EspressoDevNode runs a local Espresso node
/// for testing purposes.
pub struct EspressoDevNode {
    pub client: EspressoClient,
    _lifecycle_permit: Option<OwnedSemaphorePermit>,
}

fn compose_lifecycle_semaphore() -> &'static std::sync::Arc<Semaphore> {
    static SEMAPHORE: OnceLock<std::sync::Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| std::sync::Arc::new(Semaphore::new(1)))
}

impl Drop for EspressoDevNode {
    fn drop(&mut self) {
        // Stop the container first, then release the permit.
        // This ensures the old instance is fully stopped before another one is allowed to start.
        self.stop();
        self._lifecycle_permit.take();
    }
}

impl EspressoDevNode {
    pub fn new(client: EspressoClient) -> Self {
        Self {
            client,
            _lifecycle_permit: None,
        }
    }

    pub async fn start() -> Self {
        let lifecycle_permit = compose_lifecycle_semaphore()
            .clone()
            .acquire_owned()
            .await
            .expect("compose lifecycle semaphore closed");

        // Check if the dev node is already running
        let output = Command::new("docker")
            .args([
                "compose",
                "-f",
                COMPOSE_FILE,
                "ps",
                "--status",
                "running",
                "-q",
            ])
            .output()
            .expect("failed to run docker compose - is docker running?");

        let already_running =
            output.status.success() && !String::from_utf8_lossy(&output.stdout).trim().is_empty();

        if !already_running {
            // Remove any stale containers and volumes from previous runs to avoid
            // name conflicts and inherited state.
            let _ = Command::new("docker")
                .args([
                    "compose",
                    "-f",
                    COMPOSE_FILE,
                    "down",
                    "-v",
                    "--remove-orphans",
                ])
                .status();

            let status = Command::new("docker")
                .args(["compose", "-f", COMPOSE_FILE, "up", "-d", "--wait"])
                .status()
                .expect("failed to run docker compose - is docker running?");

            if !status.success() {
                panic!("docker compose up failed");
            }
        }
        let client = EspressoClient::new(
            format!("http://localhost:{ESPRESSO_SEQUENCER_API_PORT}/"),
            30,
        );
        let node = Self {
            client,
            _lifecycle_permit: Some(lifecycle_permit),
        };
        node.wait_until_ready().await;
        node
    }

    /// Polls `status/block-height` until the node reports a height > 0.
    async fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if Instant::now() >= deadline {
                self.stop();
                panic!("timed out waiting for node to be ready");
            }
            match self.client.fetch_latest_hotshot_block_height().await {
                Ok(height) if height > 0 => break,
                _ => sleep(Duration::from_millis(100)).await,
            }
        }
    }

    pub fn stop(&self) {
        let _ = Command::new("docker")
            .args(["compose", "-f", COMPOSE_FILE, "down", "-v"])
            .status()
            .expect("failed to run docker compose - is docker running?");
    }
}
