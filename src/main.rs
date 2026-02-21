pub mod espresso_client;
pub mod espresso_e2e;
pub mod rollup;
pub mod streamer;
use tracing_subscriber::{EnvFilter, fmt};

fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // TODO: fix main file
    println!("Hello, world!");
}
