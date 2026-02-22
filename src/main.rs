pub mod config;
pub mod espresso_client;
pub mod espresso_e2e;
pub mod rollup;
pub mod streamer;
pub mod utils;

use crate::config::{RollupType, ServiceConfig};
use crate::espresso_client::client::EspressoClient;
use crate::rollup::nitro::types::Nitro;
use crate::streamer::streamer::{EspressoStreamer, EspressoStreamerConfig};
use alloy::primitives::Address;
use anyhow::{Context, Result};
use clap::Parser;
use espresso_types::NamespaceId;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(name = "chain-agnostic-service")]
struct Cli {
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let config_contents = fs::read_to_string(&cli.config)
        .with_context(|| format!("failed to read config file {}", cli.config.display()))?;

    let service_config: ServiceConfig = serde_json::from_str(&config_contents)
        .with_context(|| format!("failed to parse JSON config {}", cli.config.display()))?;

    let client = EspressoClient::new(
        service_config.espresso_client.query_service_url.clone(),
        service_config.espresso_client.client_timeout_secs,
    );

    // TODO: have better defaults for some of these values
    let streamer_config = EspressoStreamerConfig {
        max_sequencer_number_drift: service_config.streamer.max_sequencer_number_drift,
        initial_backoff: Duration::from_millis(service_config.streamer.initial_backoff_ms),
        max_backoff: Duration::from_millis(service_config.streamer.max_backoff_ms),
    };

    let namespace = NamespaceId::from(service_config.rollup.namespace_id);
    let start_block = service_config.rollup.start_block;

    match service_config.rollup.rollup_type {
        RollupType::Nitro => {
            let nitro_config = service_config
                .rollup
                .nitro
                .context("rollup.nitro config is required when rollup type is nitro")?;

            let sequencer_addresses = nitro_config
                .sequencer_addresses
                .iter()
                .map(|address| {
                    Address::from_str(address)
                        .with_context(|| format!("invalid nitro sequencer address: {address}"))
                })
                .collect::<Result<Vec<_>>>()?;

            let rollup = Nitro::new(sequencer_addresses);
            let mut streamer = EspressoStreamer::new(client, rollup, streamer_config);

            streamer.poll_hotshot_blocks(namespace, start_block).await;
        }
    }

    Ok(())
}
