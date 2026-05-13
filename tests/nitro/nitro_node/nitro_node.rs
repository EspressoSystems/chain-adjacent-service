use reqwest::Url;
use std::{
    process::{Command, Stdio},
    sync::OnceLock,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const REFERENCE_DA_ENDPOINT: i32 = 9880;
const TESTNODE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/nitro-testnode");
const NITRO_NODE_VERSION: &str = "offchainlabs/nitro-node:v3.10.0-b1cf6db";

#[derive(Debug, Clone, Default)]
pub struct NitroNodeConfig {
    pub reference_da_url: Option<Url>,
    // If true, the node will be started with `--simple`,
    // and all components will run in a single container.
    pub simple: bool,
    // if true, will set up a validator
    pub validator: bool,

    pub no_l2_traffic: bool,
}

impl NitroNodeConfig {
    pub fn default_reference_da() -> Self {
        let url = format!("http://localhost:{REFERENCE_DA_ENDPOINT}")
            .parse()
            .expect("valid URL");
        Self {
            reference_da_url: Some(url),
            ..Default::default()
        }
    }
}

/// NitroNode runs a local Arbitrum Nitro node
/// for testing purposes.
pub struct NitroNode {
    pub config: NitroNodeConfig,
    _lifecycle_permit: Option<OwnedSemaphorePermit>,
}

fn compose_lifecycle_semaphore() -> &'static std::sync::Arc<Semaphore> {
    static SEMAPHORE: OnceLock<std::sync::Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE.get_or_init(|| std::sync::Arc::new(Semaphore::new(2)))
}

impl NitroNode {
    /// Brings up L1 + sequencer (+ ref DA if configured). The poster is
    /// **not** started here — call [`Self::start_poster`] once the
    /// dependencies it needs (e.g. CAS) are reachable.
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

        // `--no-run` skips the final `docker compose up $NODES` in
        // test-node.bash, but the init steps still bring up geth (L1),
        // the sequencer, and referenceda-provider when `--l2-referenceda`
        // is set. Poster is left for `start_poster()`.
        let mut args = vec!["--init-force", "--no-tokenbridge", "--no-run", "--detach"];

        if !config.simple {
            args.push("--no-simple");
        }

        if config.reference_da_url.is_some() {
            args.push("--l2-referenceda");
        }

        if config.validator {
            args.push("--validate");
        }

        if config.no_l2_traffic {
            args.push("--no-l2-traffic");
        }

        let mut child = Command::new("bash")
            .arg("./test-node.bash")
            .args(&args)
            // critical: script resolves all its relative paths from its own repo root
            .current_dir(TESTNODE_DIR)
            .env("NITRO_NODE_VERSION", NITRO_NODE_VERSION)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect(
                "failed to spawn test-node.bash — is the nitro-testnode submodule initialized?\n\
                 Run: git submodule update --init --recursive",
            );

        // `--no-run` makes the script return as soon as init is finished
        // (geth + sequencer + ref DA up, deploy steps complete, l2 traffic
        // generator backgrounded). Block on it instead of polling HTTP —
        // by the time it exits, every dependency we care about is live.
        // spawn_blocking is required because std::process::Child::wait
        // would otherwise block the tokio runtime.
        println!("Waiting for test-node.bash to finish init...");
        let status = tokio::task::spawn_blocking(move || child.wait())
            .await
            .expect("test-node.bash wait task panicked")
            .expect("failed to wait on test-node.bash");
        assert!(
            status.success(),
            "test-node.bash exited with non-zero status: {status}"
        );
        println!("test-node.bash finished init");

        Self {
            config,
            _lifecycle_permit: Some(lifecycle_permit),
        }
    }

    /// Brings up the `poster` service via `docker compose up --wait poster`.
    ///
    /// `start()` runs `test-node.bash --no-run`, which initialises L1 +
    /// sequencer (+ ref DA if configured) but skips the poster. Call this
    /// after CAS is up so the poster's overridden command (subscribing to
    /// CAS's feed and DA RPC) has a live endpoint to talk to.
    pub fn start_poster(&self) {
        assert!(
            !self.config.simple,
            "start_poster is not supported in simple mode (poster runs inside the unified node)"
        );

        // Best-effort cleanup of any prior poster state — stale containers
        // or volumes from an interrupted run will otherwise be reused and
        // can leak inconsistent state into the new run.
        let _ = Command::new("docker")
            .args(["compose", "down", "--volumes", "poster"])
            .current_dir(TESTNODE_DIR)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status();

        let status = Command::new("docker")
            .args(["compose", "up", "--wait", "poster"])
            .current_dir(TESTNODE_DIR)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .expect("failed to run `docker compose up --wait poster`");
        assert!(status.success(), "`docker compose up --wait poster` failed");
    }

    pub fn stop(&self) {
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
        self.stop();
    }
}
