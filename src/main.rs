pub mod config;
pub mod espresso_client;
pub mod espresso_e2e;
pub mod rollup;
pub mod streamer;
pub mod utils;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // TODO: implement the logic to start CAS in nitro rollup mode

    Ok(())
}
