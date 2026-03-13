pub mod config;
pub mod da_api;
pub mod espresso_client;
pub mod espresso_e2e;
pub mod rollups;
pub mod streamer;
pub mod submitter;
pub mod utils;

use std::{collections::HashMap, str::FromStr};

use alloy::primitives::Bytes;
use anyhow::Result;

use crate::{
    config::RollupType,
    da_api::{
        config::{DaApiConfig, DaProviderConfig},
        run,
    },
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // TODO: implement the logic to start CAS in nitro rollup mode
    let mut da_providers = HashMap::new();
    da_providers.insert(
        0,
        DaProviderConfig {
            da_type_byte: Bytes::from_str("0x01").unwrap(),
            endpoint_url: "http://localhost:9880".to_string(),
            auth_token: None,
        },
    );

    let config = DaApiConfig {
        listen_addr: "127.0.0.1:8080".to_string(),
        da_providers,
        ..Default::default()
    };

    run(config, RollupType::Nitro)
        .await
        .expect("server should start");

    Ok(())
}
