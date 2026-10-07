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
use anyhow::Result;
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

/// Log filter used when `RUST_LOG` is unset.
///
/// The upstream `light_client` crate is chatty: the fallback client logs every
/// slow or failed request at info/warn, and stake-table catchup emits one line
/// per epoch plus one per replayed event (thousands of lines for a multi-epoch
/// catchup) at debug. Cap the crate at `warn`, but keep `light_client::state`
/// at `info` so the per-epoch catchup progress lines remain visible.
/// Any `RUST_LOG` value overrides this entirely.
pub const DEFAULT_LOG_FILTER: &str =
    "info,light_client=warn,light_client::state=info,hotshot=warn,stake_table=warn";

fn default_env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(DEFAULT_LOG_FILTER))
}

pub async fn cas_init() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    // Only emit ANSI colour codes when stdout is a real terminal.
    tracing_subscriber::fmt()
        .with_ansi(std::io::stdout().is_terminal())
        .with_env_filter(default_env_filter())
        .try_init()
        .ok();

    Ok(())
}
