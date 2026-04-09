use reqwest::Url;
use std::{
    process::{Child, Command, Stdio},
    sync::{Mutex, OnceLock},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, sleep};

const DAS_SERVER_PORT: u16 = 8080;
const REPO_ROOT: &str = env!("CARGO_MANIFEST_DIR");

pub struct CelestiaNode {
    pub das_server_url: Url,
    process: Mutex<Child>,
    _lifecycle_permit: Option<OwnedSemaphorePermit>,
}

fn celestia_lifecycle_semaphore() -> &'static std::sync::Arc<Semaphore> {
    static SEMAPHORE: OnceLock<std::sync::Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| std::sync::Arc::new(Semaphore::new(1)))
}

impl CelestiaNode {
    pub async fn start() -> Self {
        let lifecycle_permit = celestia_lifecycle_semaphore()
            .clone()
            .acquire_owned()
            .await
            .expect("lifecycle semaphore closed");

        let script = std::path::Path::new(REPO_ROOT).join("scripts/run-celestia-server.sh");

        assert!(
            script.exists(),
            "run-celestia-server.sh not found — expected at scripts/run-celestia-server.sh"
        );

        let child = Command::new("bash")
            .arg(&script)
            .current_dir(REPO_ROOT)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to spawn run-celestia-server.sh");

        let node = Self {
            das_server_url: Url::parse(&format!("http://localhost:{DAS_SERVER_PORT}"))
                .expect("failed to parse DAS server URL"),
            process: Mutex::new(child),
            _lifecycle_permit: Some(lifecycle_permit),
        };

        node.wait_until_ready().await;
        node
    }

    /// Polls the DAS server endpoint until it responds, matching what the script checks.
    async fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(300);
        let client = reqwest::Client::new();
        println!("Waiting for Celestia DAS server to be ready...");
        loop {
            if Instant::now() >= deadline {
                self.stop();
                panic!("timed out waiting for Celestia DAS server");
            }
            if client.get(self.das_server_url.clone()).send().await.is_ok() {
                println!("Celestia DAS server ready");
                break;
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    pub fn stop(&self) {
        if let Ok(mut child) = self.process.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }

        let _ = Command::new("pkill")
            .args(["-9", "-f", "celestia-server"])
            .status();
        let _ = Command::new("pkill").args(["-f", "celestia-appd"]).status();
        let _ = Command::new("pkill")
            .args(["-f", "celestia bridge"])
            .status();
        let _ = Command::new("docker")
            .args(["rm", "-f", "celestia-das"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Drop for CelestiaNode {
    fn drop(&mut self) {
        self.stop();
    }
}
