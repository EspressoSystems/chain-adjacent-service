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
use tracing_subscriber::{EnvFilter, filter::LevelFilter, fmt::Formatter, reload};

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

/// Handle to the active log filter, returned by [`init_logging`].
pub struct LogFilter {
    handle: reload::Handle<EnvFilter, Formatter>,
}

impl LogFilter {
    /// Switches to `filter` (`RUST_LOG` syntax). An empty or invalid filter is ignored, with a
    /// warning for invalid ones, rather than failing startup: a verbosity typo must not take
    /// the service down.
    pub fn apply_override(&self, filter: &str) {
        if filter.trim().is_empty() {
            return;
        }
        let new_filter = match parse_override(filter) {
            Ok(new_filter) => new_filter,
            Err(err) => {
                tracing::warn!(filter, %err, "ignoring invalid runtime log filter override");
                return;
            }
        };
        // Logged before switching, so it is visible even when the new filter hides `info`.
        tracing::info!(filter, source = "runtime override", "switching log filter");
        if self.handle.reload(new_filter).is_err() {
            tracing::warn!(filter, "could not apply runtime log filter override");
        }
    }
}

/// Parses a runtime override. In plain `RUST_LOG` syntax a filter with no overall level switches
/// off everything it doesn't name, so a typo such as `debgu` (read as a module name) or a
/// targeted `light_client=warn` would mute all other logs. Unless the filter sets its own level,
/// `info` is kept as the baseline.
fn parse_override(filter: &str) -> Result<EnvFilter, tracing_subscriber::filter::ParseError> {
    let sets_level = filter
        .split(',')
        .any(|directive| directive.trim().parse::<LevelFilter>().is_ok());
    if sets_level {
        EnvFilter::try_new(filter)
    } else {
        EnvFilter::try_new(format!("info,{filter}"))
    }
}

/// Initialises logging from `RUST_LOG`, defaulting to `info`.
///
/// Logging starts before the AWS secret is read, so failures while reading it are logged. The
/// secret's `log_filter` is applied afterwards with [`LogFilter::apply_override`]; that needs a
/// reload because the global logger can only be installed once. Nitro deployments use it to
/// change verbosity without rebuilding the image, where `RUST_LOG` is baked into PCR0.
pub fn init_logging() -> LogFilter {
    let (filter, source) = match std::env::var("RUST_LOG") {
        Ok(filter) => (filter, "RUST_LOG env"),
        Err(_) => ("info".to_owned(), "built-in default"),
    };
    // Only emit ANSI colour codes when stdout is a real terminal.
    let builder = tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_env_filter(EnvFilter::new(&filter))
        .with_filter_reloading();
    let handle = builder.reload_handle();
    builder.try_init().ok();
    tracing::info!(filter, source, "logging initialised");
    LogFilter { handle }
}

#[cfg(test)]
mod log_filter_tests {
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::{Layer, layer::SubscriberExt};

    use super::parse_override;

    /// Records the targets of the events a filter lets through.
    struct Capture(Arc<Mutex<Vec<&'static str>>>);

    impl<S: tracing::Subscriber> Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0.lock().unwrap().push(event.metadata().target());
        }
    }

    /// Emits one `info` event from CAS and one from the light client under `filter`.
    fn passes(filter: &str) -> Vec<&'static str> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry()
            .with(Capture(seen.clone()).with_filter(parse_override(filter).unwrap()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "chain_adjacent_service", "cas");
            tracing::info!(target: "light_client::state", "light client");
        });
        seen.lock().unwrap().clone()
    }

    #[test]
    fn level_override_applies_to_everything() {
        assert!(passes("warn").is_empty());
    }

    #[test]
    fn typo_does_not_silence_other_logs() {
        for typo in ["debgu", "bogus="] {
            assert_eq!(
                passes(typo),
                ["chain_adjacent_service", "light_client::state"],
                "{typo}"
            );
        }
    }

    #[test]
    fn module_override_keeps_info_elsewhere() {
        assert_eq!(passes("light_client=warn"), ["chain_adjacent_service"]);
    }
}
