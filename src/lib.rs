pub mod config;
pub mod da_api;
pub mod espresso_client;
pub mod espresso_e2e;
pub mod rollups;
pub mod streamer;
pub mod submitter;
pub mod utils;

use anyhow::Result;

pub async fn cas_init() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // TODO: implement the logic to start CAS in nitro rollup mode

    Ok(())
}
