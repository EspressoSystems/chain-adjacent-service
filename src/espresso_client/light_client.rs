//! Trustless consumption of Espresso data via the `light-client` crate. The write side
//! (submission, self-confirmation) stays on the plain query-service client.

use std::path::PathBuf;
use std::sync::Arc;

use espresso_types::{NamespaceId, Transaction};
use light_client::LightClient;
use light_client::client::QueryServiceClient;
use light_client::state::Genesis;
use light_client::storage::{LightClientSqliteOptions, SqliteStorage};
use reqwest::Url;
use thiserror::Error;

use crate::espresso_client::types::NamespaceTransactionsInRange;
#[cfg(test)]
use serde_json::Value;

/// Errors from the trustless read path.
#[derive(Debug, Error)]
pub enum LightClientError {
    /// Opening the verified-state storage failed.
    #[error("failed to open light client storage: {0}")]
    Storage(anyhow::Error),

    /// A requested block height did not fit in `usize` on this platform.
    #[error("block height {0} exceeds usize")]
    HeightOverflow(u64),

    /// Fetching or verifying data against HotShot consensus failed — e.g. an unreachable or
    /// dishonest query node, or a proof that did not verify against the stake table.
    #[error("verified fetch failed: {0}")]
    Verification(anyhow::Error),
}

/// A read-only, trustless view of Espresso data: every block it returns is verified against
/// HotShot consensus (rooted in the configured [`Genesis`]), so the query node is untrusted.
///
/// To fail over across multiple query nodes, generalize over `S: Client` and build a
/// `FallbackClient<QueryServiceClient>` in [`new`](Self::new); the read methods are unaffected.
#[derive(Clone)]
pub struct LightClientReader {
    inner: Arc<LightClient<SqliteStorage, QueryServiceClient>>,
}

impl LightClientReader {
    /// `genesis` is the root of trust and must match the network `query_url` serves.
    /// `db_path` persists the verified-state cache across restarts; `None` keeps it in memory
    /// (rebuilt via catch-up each start).
    pub async fn new(
        genesis: Genesis,
        query_url: Url,
        db_path: Option<PathBuf>,
    ) -> Result<Self, LightClientError> {
        let storage = LightClientSqliteOptions {
            lc_path: db_path,
            ..Default::default()
        }
        .connect()
        .await
        .map_err(LightClientError::Storage)?;

        let server = QueryServiceClient::new(query_url);
        let inner = LightClient::from_genesis(storage, server, genesis);

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Latest verified HotShot block height. May underestimate (the light client never
    /// reports a height it hasn't verified); the streamer tolerates this by polling again.
    pub async fn block_height(&self) -> Result<u64, LightClientError> {
        self.inner
            .block_height()
            .await
            .map_err(LightClientError::Verification)
    }

    /// Verified namespace transactions for the half-open range `[start, end)` — one entry
    /// per height (including empty blocks), which the streamer's positional parsing relies on.
    ///
    /// `proof` is always `None`: inclusion was already verified inside the light client, and
    /// nothing downstream reads that field.
    pub async fn namespace_transactions_in_range(
        &self,
        namespace: NamespaceId,
        start: u64,
        end: u64,
    ) -> Result<Vec<NamespaceTransactionsInRange>, LightClientError> {
        let start_usize =
            usize::try_from(start).map_err(|_| LightClientError::HeightOverflow(start))?;
        let end_usize = usize::try_from(end).map_err(|_| LightClientError::HeightOverflow(end))?;
        let verified: Vec<Vec<Transaction>> = self
            .inner
            .fetch_namespaces_in_range(start_usize, end_usize, namespace)
            .await
            .map_err(LightClientError::Verification)?;

        Ok(verified
            .into_iter()
            .map(|transactions| NamespaceTransactionsInRange {
                transactions,
                proof: None,
            })
            .collect())
    }
}

#[cfg(test)]
impl LightClientReader {
    /// In-memory reader with an empty genesis, for tests that exercise streamer queue logic
    /// without fetching data. Verification against it is meaningless — don't read real blocks.
    pub async fn new_for_test(query_url: Url) -> Self {
        let genesis = serde_json::from_str(
            r#"{"epoch_height":100,"first_epoch_with_dynamic_stake_table":1,"stake_table":[]}"#,
        )
        .expect("valid test genesis");
        Self::new(genesis, query_url, None)
            .await
            .expect("failed to build in-memory test light client")
    }
}

/// Derive a genesis JSON from a node's `/config/hotshot` — stake table + epoch params, with
/// `first_epoch = epoch_start_block / epoch_height + 3` (validated against decaf's published
/// genesis and a real mainnet run). Lets tests derive the trusted genesis instead of
/// committing validator-key blobs. Returns JSON so tests can mutate it (e.g. the negative
/// test swapping in a wrong stake table) before deserializing.
#[cfg(test)]
pub(crate) async fn genesis_json_from_node(query_url: &Url) -> Value {
    let config_url = query_url.join("config/hotshot").expect("join config url");
    let response: Value = reqwest::Client::new()
        .get(config_url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
        .expect("fetch /config/hotshot")
        .json()
        .await
        .expect("parse /config/hotshot");

    let config = &response["config"];
    let epoch_height = config["epoch_height"]
        .as_u64()
        .expect("config.epoch_height");
    let epoch_start_block = config["epoch_start_block"]
        .as_u64()
        .expect("config.epoch_start_block");
    let stake_table: Vec<Value> = config["known_nodes_with_stake"]
        .as_array()
        .expect("config.known_nodes_with_stake")
        .iter()
        .map(|node| node["stake_table_entry"].clone())
        .collect();

    serde_json::json!({
        "epoch_height": epoch_height,
        "first_epoch_with_dynamic_stake_table": epoch_start_block / epoch_height + 3,
        "stake_table": stake_table,
    })
}

#[cfg(test)]
pub(crate) async fn genesis_from_node(query_url: &Url) -> Genesis {
    serde_json::from_value(genesis_json_from_node(query_url).await)
        .expect("genesis from node config")
}

/// End-to-end verification against live Espresso nodes (network / Docker), so all are
/// `#[ignore]`d. Run one with:
///   cargo test -p chain-adjacent-service <name> -- --ignored --nocapture
#[cfg(test)]
mod live_verification {
    use super::*;

    const MAINNET_URL: &str = "https://query.main.net.espresso.network/";
    const DECAF_URL: &str = "https://query.decaf.testnet.espresso.network/";
    const DEVNODE_URL: &str = "http://localhost:41000/";

    // First block inside mainnet's first dynamic-stake-table epoch (minimal catch-up).
    const MAINNET_DEFAULT_START: u64 = 10_960_300; // epoch 277 * 40_000

    async fn reader(url: &str) -> LightClientReader {
        let url = Url::parse(url).unwrap();
        LightClientReader::new(genesis_from_node(&url).await, url, None)
            .await
            .expect("build reader")
    }

    // Surface the light client's catch-up/verification tracing to test output.
    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
    }

    /// Fetch `[start, start+3)` and assert one verified entry per height. Any namespace works
    /// (absent ones return a verified absence proof), so it needn't exist on the network.
    async fn assert_verifies_range(reader: &LightClientReader, label: &str, start: u64) {
        let end = start + 3;
        let blocks = reader
            .namespace_transactions_in_range(NamespaceId::from(1u64), start, end)
            .await
            .expect("verified namespace range");

        let txs: usize = blocks.iter().map(|b| b.transactions.len()).sum();
        println!(
            "{label} verified [{start}, {end}): {} blocks, {txs} txs",
            blocks.len()
        );
        assert_eq!(blocks.len(), (end - start) as usize);
    }

    // Production case. `MAINNET_SMOKE_START` overrides the height to test deeper catch-up.
    #[tokio::test]
    #[ignore = "hits the public mainnet node; run with --ignored"]
    async fn mainnet_verifies_namespace_range() {
        init_tracing();
        let start = std::env::var("MAINNET_SMOKE_START")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(MAINNET_DEFAULT_START);
        let reader = reader(MAINNET_URL).await;
        assert_verifies_range(&reader, "MAINNET", start).await;
    }

    // mainnet's epoch params but decaf's (real, valid, foreign) validators → mainnet's proofs
    // MUST be rejected (a BLS pairing failure). Proves verification isn't a no-op.
    #[tokio::test]
    #[ignore = "hits the public mainnet + decaf nodes; run with --ignored"]
    async fn mainnet_rejects_wrong_stake_table() {
        init_tracing();
        let mut g = genesis_json_from_node(&Url::parse(MAINNET_URL).unwrap()).await;
        g["stake_table"] =
            genesis_json_from_node(&Url::parse(DECAF_URL).unwrap()).await["stake_table"].clone();
        let bad: Genesis = serde_json::from_value(g).unwrap();

        let reader = LightClientReader::new(bad, Url::parse(MAINNET_URL).unwrap(), None)
            .await
            .expect("build reader");
        let res = reader
            .namespace_transactions_in_range(
                NamespaceId::from(1u64),
                MAINNET_DEFAULT_START,
                MAINNET_DEFAULT_START + 3,
            )
            .await;

        println!("wrong-stake-table result: {res:?}");
        assert!(
            res.is_err(),
            "must reject mainnet proofs under a wrong stake table"
        );
    }

    // Local dev node; needs a `/light-client`-enabled image (PR #4453) running via
    // `docker compose -f src/espresso_e2e/docker-compose.yml up -d`. No catch-up (static set).
    #[tokio::test]
    #[ignore = "needs dockerized dev node serving /light-client; run with --ignored"]
    async fn devnode_verifies_namespace_range() {
        let reader = reader(DEVNODE_URL).await;
        let height = reader.block_height().await.expect("verified block height");
        assert!(height > 2, "dev node should have produced some blocks");
        assert_verifies_range(&reader, "DEV NODE", 1).await;
    }
}
