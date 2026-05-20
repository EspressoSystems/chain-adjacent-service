use std::sync::Arc;

use alloy::{
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
};
use anyhow::Result;
use chain_adjacent_service::config::RollupType;
use chain_adjacent_service::da_api;
use chain_adjacent_service::espresso_client::client::EspressoClient;
use chain_adjacent_service::key_manager::attestation_client::HttpAttestationVerifierClient;
use chain_adjacent_service::key_manager::key_manager::KeyManager;
use chain_adjacent_service::key_manager::tee_verifier::TEEVerifier;
use chain_adjacent_service::rollups::nitro::types::Nitro;
use chain_adjacent_service::rollups::rollup::{BatchCursorFetcher, L1Monitor};
use chain_adjacent_service::secrets::{
    apply_overrides_nitro, assert_no_placeholders_nitro, fetch_secret_overrides,
};
use chain_adjacent_service::streamer::streamer::Streamer;
use chain_adjacent_service::{cas_init, config::ServiceConfig, rollups::rollup::Rollup};

use chain_adjacent_service::submitter::submitter::Submitter;
use clap::Parser;
use espresso_types::NamespaceId;
use tokio::sync::{mpsc, watch};
use tracing::info;

#[derive(Parser)]
struct Cli {
    /// Path to the JSON config file
    #[arg(short, long, env = "CAS_CONFIG")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    cas_init().await?;

    let cli = Cli::parse();
    let config_contents = std::fs::read_to_string(&cli.config)?;
    let config: ServiceConfig<serde_json::Value> = serde_json::from_str(&config_contents)?;

    match config.rollup.ty {
        RollupType::Nitro => {
            let mut config: ServiceConfig<<Nitro as Rollup>::StackConfig> =
                serde_json::from_str(&config_contents)?;

            if let Some(overrides) = fetch_secret_overrides(config.key_manager.tee_type).await? {
                apply_overrides_nitro(&mut config, &overrides)?;
            }
            assert_no_placeholders_nitro(&config)?;

            let operator_private_key = std::env::var("OPERATOR_PRIVATE_KEY").map_err(|_| {
                anyhow::anyhow!(
                    "OPERATOR_PRIVATE_KEY env var must be set when key_manager is configured"
                )
            })?;
            let operator_signer: PrivateKeySigner = operator_private_key.parse()?;
            let tee_verifier = TEEVerifier::new(
                config.key_manager.rpc_url.clone(),
                config.key_manager.tee_verifier_address,
                operator_signer,
            );
            let attestation_client = HttpAttestationVerifierClient::new(
                config.key_manager.attestation_verifier_url.clone(),
                config.key_manager.attestation_client_timeout_secs,
            )?;

            let provider = ProviderBuilder::new().connect_http(config.key_manager.rpc_url.clone());
            let parent_chain_id = provider.get_chain_id().await?;

            let signer = PrivateKeySigner::random();
            let mut key_manager = KeyManager::new(
                Box::new(tee_verifier),
                Box::new(attestation_client),
                config.key_manager.max_register_attempts,
                config.key_manager.tee_type.into(),
                parent_chain_id,
                config.key_manager.tee_verifier_address,
                signer,
            )?;
            key_manager.initialize().await?;
            tracing::info!(
                "TEE key registered, signer address: {:?}",
                key_manager.signer().address()
            );

            let l1_monitor = Nitro::create_l1_monitor(&config.rollup.stack).await?;
            let cursor_fetcher: Option<Arc<dyn BatchCursorFetcher>> =
                Some(l1_monitor.create_cursor_fetcher());

            run::<Nitro>(config, key_manager, l1_monitor, cursor_fetcher).await
        }
    }
}

async fn run<R: Rollup>(
    config: ServiceConfig<R::StackConfig>,
    key_manager: KeyManager,
    l1_monitor: R::L1Monitor,
    cursor_fetcher: Option<Arc<dyn BatchCursorFetcher>>,
) -> Result<()> {
    info!("L1 monitor created");

    let (batch_cursor, hotshot_height) = if !config.is_fresh_deployment {
        let checkpoint = l1_monitor.fetch_latest_checkpoint_on_startup().await?;
        (checkpoint.batch_cursor, Some(checkpoint.hotshot_height))
    } else {
        let batch_cursor = l1_monitor
            .fetch_latest_batch_cursor_on_fresh_deployment()
            .await?;
        (batch_cursor, None)
    };

    let config = R::resolve_config_with_checkpoint(config, batch_cursor.clone(), hotshot_height);

    let client = EspressoClient::from_config(config.espresso_client.clone());
    let (submitter_sender, submitter_receiver) = mpsc::channel::<R::FeedMessage>(100);

    let build_tx_fn = |namespace_id: NamespaceId, msgs: Vec<R::FeedMessage>| {
        let mut msgs = msgs;
        let mut txes = Vec::new();
        while !msgs.is_empty() {
            let payload = R::build_espresso_tx_payload(&mut msgs);
            if payload.is_empty() {
                panic!("build_espresso_tx_payload returned empty payload for non-empty messages");
            }
            let tx = espresso_types::Transaction::new(namespace_id, payload);
            txes.push(tx);
        }
        txes
    };
    let namespace_id = NamespaceId::from(config.rollup.namespace_id);

    let mut submitter = Submitter::new(
        client,
        submitter_receiver,
        config.submitter,
        namespace_id,
        build_tx_fn,
    );
    let submitter_task = submitter.submit_transactions();

    let (l1_finalized_msg_idx_sender, l1_finalized_msg_idx_receiver) = watch::channel(0u64);

    let (espresso_finalization_sender, espresso_finalization_receiver) =
        mpsc::channel(config.advanced.espresso_finalized_message_channel_capacity);

    let feed_task = R::start_feed_relay(
        config.rollup.stack.clone(),
        submitter_sender,
        espresso_finalization_receiver,
        l1_finalized_msg_idx_receiver.clone(),
    );

    let client = EspressoClient::from_config(config.espresso_client);
    let (verification_sender, verification_receiver) =
        mpsc::channel(config.advanced.verification_channel_capacity);
    let mut streamer: Streamer<R> = Streamer::new(
        client,
        config.streamer,
        config.rollup,
        config.advanced,
        cursor_fetcher,
    );

    let streamer_task = streamer.run(
        l1_finalized_msg_idx_receiver,
        verification_receiver,
        espresso_finalization_sender,
    );

    let da_task = da_api::run(
        config.da_server,
        R::rollup_type(),
        verification_sender,
        Arc::new(key_manager),
    );
    info!("DA API server started");

    let l1_monitor_task = l1_monitor.start(l1_finalized_msg_idx_sender);

    tokio::try_join!(
        async { submitter_task.await.map_err(anyhow::Error::from) },
        async { feed_task.await.map_err(anyhow::Error::from) },
        async {
            streamer_task.await;
            Ok::<(), anyhow::Error>(())
        },
        async { da_task.await.map_err(anyhow::Error::from) },
        async {
            l1_monitor_task.await;
            Ok::<(), anyhow::Error>(())
        },
    )?;
    Ok(())
}
