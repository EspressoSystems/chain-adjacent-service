use anyhow::Result;
use chain_agnostic_service::cas_init;

#[tokio::main]
async fn main() -> Result<()> {
    cas_init().await
}
