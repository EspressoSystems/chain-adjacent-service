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

/// Derive the trusted genesis from a node's `/config/hotshot` — stake table + epoch params,
/// with `first_epoch = epoch_start_block / epoch_height + 3`. Lets tests derive the genesis
/// from the node they verify against rather than committing validator-key blobs.
#[cfg(test)]
pub(crate) async fn genesis_from_node(query_url: &Url) -> Genesis {
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

    serde_json::from_value(serde_json::json!({
        "epoch_height": epoch_height,
        "first_epoch_with_dynamic_stake_table": epoch_start_block / epoch_height + 3,
        "stake_table": stake_table,
    }))
    .expect("build genesis from node config")
}

/// Verifies the reader against a dockerized dev node (started in-test via `EspressoDevNode`,
/// so it runs in CI). Genesis-regime only. Decaf on the devnet covers dynamic catch-up.
#[cfg(test)]
mod dev_node {
    use std::time::Duration;

    use tokio::time::{Instant, sleep};

    use super::*;
    use crate::espresso_e2e::espresso_dev_node::EspressoDevNode;

    /// Verified tx payloads for `namespace` over `[0, height)`.
    async fn payloads(
        reader: &LightClientReader,
        namespace: NamespaceId,
        height: u64,
    ) -> Vec<Vec<u8>> {
        if height == 0 {
            return Vec::new();
        }
        reader
            .namespace_transactions_in_range(namespace, 0, height)
            .await
            .expect("verified namespace range")
            .into_iter()
            .flat_map(|block| block.transactions)
            .map(|tx| tx.payload().to_vec())
            .collect()
    }

    // Each namespace returns exactly its own verified tx (content + isolation); an unused
    // namespace verifies as empty (absence).
    #[tokio::test]
    async fn verifies_namespace_content_isolation_and_absence() {
        let node = EspressoDevNode::start().await;
        let url = node.client.config.base_url.clone();
        let reader = LightClientReader::new(genesis_from_node(&url).await, url, None)
            .await
            .expect("build reader");

        let ns_a = NamespaceId::from(1u64);
        let ns_b = NamespaceId::from(2u64);
        let ns_unused = NamespaceId::from(3u64);
        let tx_a = b"tx-a".to_vec();
        let tx_b = b"tx-b".to_vec();
        for (ns, tx) in [(ns_a, &tx_a), (ns_b, &tx_b)] {
            node.client
                .submit_transaction(Transaction::new(ns, tx.clone()))
                .await
                .expect("submit transaction");
        }

        // Wait until both submitted txs are verified-visible (exits as soon as they appear).
        let deadline = Instant::now() + Duration::from_secs(30);
        let height = loop {
            let height = reader.block_height().await.expect("verified block height");
            if !payloads(&reader, ns_a, height).await.is_empty()
                && !payloads(&reader, ns_b, height).await.is_empty()
            {
                break height;
            }
            assert!(
                Instant::now() < deadline,
                "submitted txs not verified in time"
            );
            sleep(Duration::from_millis(500)).await;
        };

        assert_eq!(payloads(&reader, ns_a, height).await, vec![tx_a]);
        assert_eq!(payloads(&reader, ns_b, height).await, vec![tx_b]);
        let unused = payloads(&reader, ns_unused, height).await;
        assert!(unused.is_empty(), "unused namespace must verify as empty");
    }
}
