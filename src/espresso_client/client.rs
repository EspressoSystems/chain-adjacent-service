use std::time::Duration;

use crate::espresso_client::types::{
    LimitsData, NamespaceTransactionsInRange, TransactionQueryData,
};
use anyhow::{Result, bail};
use committable::Commitment;
use espresso_types::{NamespaceId, Transaction};
use reqwest::Url;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Configuration for the Espresso client.
#[derive(Debug, Clone)]
pub struct Config {
    /// Base URL of the Espresso Query Service (must end with `/`).
    pub query_base_url: Url,
}

/// HTTP client for the Espresso Query Service API.
///
/// Provides typed methods for submitting transactions
/// and querying block/transaction data from the Espresso network.
#[derive(Debug, Clone)]
pub struct EspressoClient {
    pub config: Config,
    pub client: reqwest::Client,
}

impl EspressoClient {
    pub fn new(mut query_base_url: String, timeout: u64) -> Self {
        if !query_base_url.ends_with('/') {
            query_base_url.push('/')
        }

        Self {
            config: Config {
                query_base_url: Url::parse(&query_base_url)
                    .expect("query service base url is incorrect"),
            },
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(timeout))
                .build()
                .expect("failed to build espresso client"),
        }
    }

    /// Submits a transaction to the Espresso sequencer via `POST /submit/submit`.
    /// Returns the transaction's commitment hash which can be used for lookups.
    pub async fn submit_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Commitment<Transaction>> {
        let url = self.config.query_base_url.join("submit/submit")?;
        self.post(url, transaction).await
    }

    /// Fetches all namespace-filtered transactions across a block range via
    /// `GET /availability/block/{start}/{end}/namespace/{namespace}`.
    ///
    /// Validates the requested range against the server's `large_object_range_limit`
    /// before making the request.
    pub async fn fetch_namespace_transactions_in_range(
        &self,
        namespace: NamespaceId,
        start: u64,
        end: u64,
    ) -> Result<Vec<NamespaceTransactionsInRange>> {
        let limits = self.fetch_limits().await?;

        if end - start > limits.large_object_range_limit {
            bail!(
                "requested block range exceeds maximum allowed {}",
                limits.large_object_range_limit
            );
        }

        let url = self.config.query_base_url.join(&format!(
            "availability/block/{start}/{end}/namespace/{namespace}"
        ))?;
        self.get(url).await
    }

    /// Fetches rate-limiting and range constraints from `GET /availability/limits`.
    pub async fn fetch_limits(&self) -> Result<LimitsData> {
        let url = self.config.query_base_url.join("availability/limits")?;
        self.get(url).await
    }

    /// Returns the latest finalized HotShot block height via `GET /status/block-height`.
    pub async fn fetch_latest_hotshot_block_height(&self) -> Result<u64> {
        let url = self.config.query_base_url.join("status/block-height")?;
        self.get(url).await
    }

    /// Looks up a transaction by its commitment hash via
    /// `GET /availability/transaction/hash/{hash}/noproof`.
    ///
    /// Uses the `/noproof` variant to skip the inclusion proof and return only
    /// the transaction metadata (block height, index, namespace, etc.).
    pub async fn fetch_transaction_by_hash(
        &self,
        tx_hash: Commitment<Transaction>,
    ) -> Result<TransactionQueryData> {
        let url = self
            .config
            .query_base_url
            .join(&format!("availability/transaction/hash/{tx_hash}/noproof"))?;

        self.get(url).await
    }

    async fn get<A>(&self, url: Url) -> Result<A>
    where
        A: DeserializeOwned,
    {
        let res = self.client.get(url).send().await?;
        if !res.status().is_success() {
            bail!("request failed with status {}", res.status());
        }
        Ok(res.json().await?)
    }

    async fn post<A, T>(&self, url: Url, tx: A) -> Result<T>
    where
        A: Serialize,
        T: DeserializeOwned,
    {
        let res = self.client.post(url).json(&tx).send().await?;
        if !res.status().is_success() {
            bail!("request failed with status {}", res.status());
        }

        Ok(res.json().await?)
    }
}

#[cfg(test)]
pub mod testing {
    use super::*;
    use crate::espresso_e2e::espresso_dev_node::EspressoDevNode;
    use base64::Engine;
    use base64::engine::general_purpose;

    #[tokio::test]
    async fn test_espresso_client() {
        let node = EspressoDevNode::start().await;

        let height = node
            .client
            .fetch_latest_hotshot_block_height()
            .await
            .expect("failed to fetch block height");
        assert!(height > 0, "expected block height > 0, got {height}");

        // Verify the query service reports expected rate limits
        let limits = node
            .client
            .fetch_limits()
            .await
            .expect("failed to fetch limits");
        assert!(
            limits.large_object_range_limit == 100,
            "expected large object range limit to be 100, got {}",
            limits.large_object_range_limit
        );
        assert!(
            limits.small_object_range_limit == 500,
            "expected small object range limit to be 500, got {}",
            limits.small_object_range_limit
        );

        let base64_tx = "AAAAAAAAAEHUzp83ExZ0YX8z8UFsdR0WWl35/YUD4ztKgVRORdSUtAquzMVHSNYB+6xMTYjjq6/Cq/7T47y32tg3c89ydiSCAAAAAAAAFx0wAAAAAAAAAWP5AWD5AVnhA5SksAAAAAAAAAAAAHNlcXVlbmNlcoOdQgeEaZmUXsCAuQE0BPkBMIMFm5qEBycOAIMw1ACUixTSh7QVD/Iqxz34vnIOkz9lmryAuMQxYbf2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACgbAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABPpjKCh4D0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAN+EdYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQhOTC5Pagy3CAh+JRRJV8Hc9wfa+tf561YI9RcLY0UoQe6rG4bP2gdKXEWOeff78tBe6lSjTdNPcqtpmISDbe+np1LYHs7YWDAfFsAAAAAAAXHTEAAAAAAAACqPkCpfkCnuEDlKSwAAAAAAAAAAAAc2VxdWVuY2Vyg51CB4RpmZRewIC5AnkDAAAAAAAAATQE+QEwgwWbm4QHJw4AgzDUAJSLFNKHtBUP8irHPfi+cg6TP2WavIC4xDFht/YAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJ3YAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG56A3KZTVVcwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC+vCAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABCE5MLk9qBwXxE83WOuyDScPRpzNia3+O8NNlq0EpHppiZBPrps5qAcnOrAs2KZH8T6BcMGSiRlWD4geopUnHHdPtTzf2YcYAAAAAAAAAE0BPkBMIMFm5yEBycOAIMw1ACUixTSh7QVD/Iqxz34vnIOkz9lmryAuMQxYbf2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACi3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFa9cDhqGLCe8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9DPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQhOTC5Pag9dwM+FhRreQuo38RmDdEGaI29lgztzKhSab2vm8C5SmgX5u4aGhKFdQv5sJixku38BICGfaZpGqimk265fHCToCDAfFs";

        let namespace_id = NamespaceId::from(1918988905u64);
        let tx_bytes = general_purpose::STANDARD
            .decode(base64_tx)
            .expect("failed to decode base64 tx");
        let espresso_transaction = Transaction::new(namespace_id, tx_bytes);

        let tx_hash = node
            .client
            .submit_transaction(espresso_transaction)
            .await
            .expect("failed to submit transaction");
        assert!(tx_hash.to_string() == "TX~NgMAbMRT16XtCJ47yt6wSTwjGdMMFN6Z8okg1agD2waP");

        // Allow time for the transaction to be sequenced and finalized
        tokio::time::sleep(Duration::from_secs(2)).await;

        let fetched_tx = node
            .client
            .fetch_transaction_by_hash(tx_hash)
            .await
            .expect("failed to fetch transaction");

        let namepspace_transactions = node
            .client
            .fetch_namespace_transactions_in_range(namespace_id, 0, fetched_tx.block_height + 1)
            .await
            .expect("failed to fetch namespace transactions");

        // Verify the submitted payload appears in at least one block's transactions
        assert!(
            namepspace_transactions.iter().any(|block| {
                block
                    .transactions
                    .iter()
                    .any(|tx| tx.payload() == fetched_tx.transaction.payload())
            }),
            "submitted transaction payload not found in namespace block range"
        );

        node.stop();
    }
}
