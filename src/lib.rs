pub mod config;
pub mod da_api;
pub mod espresso_client;
#[cfg(any(test, feature = "e2e"))]
pub mod espresso_e2e;
pub mod key_manager;
pub mod rollups;
pub mod secrets;
pub mod streamer;
pub mod submitter;
pub mod utils;
pub mod ws_proxy_connect;

use alloy::primitives::Bytes;
use std::io::IsTerminal;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct VerificationResult {
    pub success: bool,
    pub start_message_position: u64,
    pub end_message_position: u64,
    pub start_espresso_block: u64,
    pub after_delayed_messages_read: u64,
    pub min_espresso_block_still_in_queue: u64,
}

impl VerificationResult {
    pub fn success(
        start_message_position: u64,
        end_message_position: u64,
        start_espresso_block: u64,
        after_delayed_messages_read: u64,
        min_espresso_block_still_in_queue: u64,
    ) -> Self {
        Self {
            success: true,
            start_message_position,
            end_message_position,
            start_espresso_block,
            after_delayed_messages_read,
            min_espresso_block_still_in_queue,
        }
    }

    pub fn failure() -> Self {
        Self {
            success: false,
            start_message_position: 0,
            end_message_position: 0,
            start_espresso_block: 0,
            after_delayed_messages_read: 0,
            min_espresso_block_still_in_queue: 0,
        }
    }
}

pub type VerificationSender = mpsc::Sender<(Bytes, oneshot::Sender<VerificationResult>)>;
pub type VerificationReceiver = mpsc::Receiver<(Bytes, oneshot::Sender<VerificationResult>)>;

pub fn cas_init() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
}

/// Initialises logging.
///
/// The filter spec (`RUST_LOG` syntax) is chosen in this order:
/// 1. `override_spec`, if given and valid;
/// 2. the `RUST_LOG` environment variable;
/// 3. Default to `info`.
///
/// Nitro deployments pass the `log_filter` field of the AWS secret as
/// `override_spec`, so verbosity can change without rebuilding the image,
/// where `RUST_LOG` is baked into PCR0. An unparsable override is skipped
/// with a warning instead of failing startup: a verbosity typo must not take
/// the service down.
pub fn init_logging(override_spec: Option<&str>) {
    use tracing_subscriber::EnvFilter;

    let mut rejected = None;
    let (filter, spec, source) = override_spec
        .and_then(|s| match EnvFilter::try_new(s) {
            Ok(filter) => Some((filter, s.to_owned(), "runtime override")),
            Err(err) => {
                rejected = Some((s.to_owned(), err.to_string()));
                None
            }
        })
        .unwrap_or_else(|| match std::env::var("RUST_LOG") {
            Ok(s) => (EnvFilter::new(&s), s, "RUST_LOG env"),
            Err(_) => (
                EnvFilter::new("info"),
                "info".to_owned(),
                "built-in default",
            ),
        });

    // Only emit ANSI colour codes when stdout is a real terminal.
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_env_filter(filter)
        .try_init()
        .ok();

    tracing::info!(filter = %spec, source, "logging initialised");
    if let Some((bad, err)) = rejected {
        tracing::warn!(spec = %bad, %err, "ignoring invalid runtime log filter override");
    }
}
