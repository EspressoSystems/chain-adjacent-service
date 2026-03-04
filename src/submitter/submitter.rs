use std::sync::Arc;
use std::time::Duration;

use espresso_types::Transaction;
use tokio::sync::{AcquireError, Semaphore, mpsc};

use crate::espresso_client::client::EspressoClient;
use crate::utils::exponential_backoff;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct SubmitterConfig {
    pub max_in_flight: usize,
    pub finalization_wait_ms: u64,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub max_finalization_poll_retries: u32,
}

#[derive(Debug, Error)]
pub enum SubmitterError {
    #[error("channel was closed")]
    ChannelClosed,
    #[error("acquire permit error: {0}")]
    FailedToAcquirePermit(AcquireError),
}

/// Submitter is responsible for submitting transactions to Espresso
/// by reading transactions from `unsubmitted_txs` mpsc channel.
/// After submitting a transaction, it checks for its finalization status.
/// before submitting the next transaction.
pub struct Submitter {
    client: EspressoClient,
    unsubmitted_txs: mpsc::Receiver<Transaction>,
    config: SubmitterConfig,
    pub(crate) sem: Arc<Semaphore>,
}

impl Submitter {
    pub fn new(
        client: EspressoClient,
        channel: mpsc::Receiver<Transaction>,
        config: SubmitterConfig,
    ) -> Self {
        let sem = Arc::new(Semaphore::new(config.max_in_flight));
        Self {
            client,
            unsubmitted_txs: channel,
            config,
            sem,
        }
    }
    /// Submit transactions first reads transactions from the `unsubmitted_txs` channel.
    ///
    /// Then it acquires a permit from the semaphore, blocking if all max_in_flight is hit.
    ///
    /// Finally, it submits the transaction to Espresso and checks the finalization of the transaction.
    pub async fn submit_transactions(&mut self) -> Result<(), SubmitterError> {
        loop {
            // First try to fetch a transaction from the channel
            let tx = self
                .unsubmitted_txs
                .recv()
                .await
                .ok_or(SubmitterError::ChannelClosed)?;
            // Acquire a permit from semaphore, block if all max_in_flight is hit
            let permit = self
                .sem
                .clone()
                .acquire_owned()
                .await
                .map_err(SubmitterError::FailedToAcquirePermit)?;
            let client = self.client.clone();
            let config = self.config.clone();
            tokio::spawn(async move {
                // move the permit to this spawned task so that its automatically released when the task completes
                let _permit = permit;
                loop {
                    let mut backoff = Duration::from_millis(config.initial_backoff_ms);

                    // 1. Submit the transaction, retrying on failure.
                    let tx_hash = loop {
                        match client.submit_transaction(tx.clone()).await {
                            Ok(tx_hash) => break tx_hash,
                            Err(err) => {
                                tracing::warn!("failed to submit transaction: {err}, retrying...");
                                backoff = exponential_backoff(
                                    backoff,
                                    Duration::from_millis(config.max_backoff_ms),
                                )
                                .await;
                            }
                        }
                    };

                    // 2. Poll for finalization with max retries.
                    // Wait for a grace period before the first check.
                    tokio::time::sleep(Duration::from_millis(config.finalization_wait_ms)).await;

                    backoff = Duration::from_millis(config.initial_backoff_ms);
                    let mut poll_attempts: u32 = 0;
                    let finalized = loop {
                        match client.fetch_transaction_by_hash(tx_hash).await {
                            Ok(data) => {
                                tracing::info!("finalized transaction with hash: {:?}", data.hash);
                                break true;
                            }
                            Err(err) => {
                                poll_attempts += 1;
                                if poll_attempts >= config.max_finalization_poll_retries {
                                    tracing::warn!(
                                        "max poll retries ({}) reached for tx {tx_hash}, resubmitting",
                                        config.max_finalization_poll_retries
                                    );
                                    break false;
                                }
                                tracing::warn!(
                                    "transaction not finalized (attempt {poll_attempts}/{}), retrying: {err}",
                                    config.max_finalization_poll_retries
                                );
                                backoff = exponential_backoff(
                                    backoff,
                                    Duration::from_millis(config.max_backoff_ms),
                                )
                                .await;
                            }
                        }
                    };

                    if finalized {
                        break;
                    }
                }
            });
        }
    }
}

#[cfg(test)]
pub mod testing {
    use crate::{
        espresso_e2e::{
            espresso_dev_node::EspressoDevNode, mock_rollup::make_mock_espresso_transaction,
        },
        submitter::submitter::SubmitterError,
        submitter::submitter::{Submitter, SubmitterConfig},
    };
    use espresso_types::NamespaceId;
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_submitter_handles_load_with_limited_concurrency() {
        let espresso_node = EspressoDevNode::start().await;
        let config = SubmitterConfig {
            max_in_flight: 2,
            // 10 seconds
            finalization_wait_ms: 10000,
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
            max_finalization_poll_retries: 5,
        };
        // Max 256 transactions at a time in the channel
        let (sender, reciever) = mpsc::channel(256);
        let mut submitter = Submitter::new(espresso_node.client.clone(), reciever, config);
        let sem = submitter.sem.clone();

        // Spawn a task to send 20 transactions at ~20 tx/sec
        let send_handle = tokio::spawn(async move {
            for seq in 0..=20u64 {
                let tx = make_mock_espresso_transaction(seq);
                sender.send(tx).await.expect("failed to send tx");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        // Run the submitter until all transactions are processed
        let submitter_result =
            tokio::time::timeout(Duration::from_secs(120), submitter.submit_transactions())
                .await
                .expect("submitter should have completed all tasks within 2 minutes");
        assert!(
            matches!(submitter_result, Err(SubmitterError::ChannelClosed)),
            "submitter should stop after sender closes channel"
        );
        send_handle.await.expect("sender task panicked");
        // Wait for all in flight tasks to finish
        let wait_result = tokio::time::timeout(Duration::from_secs(60), async {
            let _ = sem
                .acquire_many(2)
                .await
                .expect("semaphore closed unexpectedly");
        })
        .await;

        assert!(
            wait_result.is_ok(),
            "in-flight tasks did not complete in time"
        );

        // Verify all 20 transactions landed on Espresso
        let namespace_id = NamespaceId::from(1918988905u64);
        let latest_height = espresso_node
            .client
            .fetch_latest_hotshot_block_height()
            .await
            .expect("failed to fetch block height");

        // Fetch namespace transactions in chunks of 100 (server limit)
        let mut found_seqs: Vec<u64> = Vec::new();
        let mut from = 0u64;
        while from <= latest_height {
            let to = std::cmp::min(from + 100, latest_height + 1);
            let blocks = espresso_node
                .client
                .fetch_namespace_transactions_in_range(namespace_id, from, to)
                .await
                .expect("failed to fetch namespace transactions");

            for block in &blocks {
                for tx in &block.transactions {
                    let payload = tx.payload();
                    if payload.len() >= 8 {
                        let mut seq_bytes = [0u8; 8];
                        seq_bytes.copy_from_slice(&payload[payload.len() - 8..]);
                        found_seqs.push(u64::from_be_bytes(seq_bytes));
                    }
                }
            }
            from = to;
        }

        for expected_seq in 0..=20u64 {
            assert!(
                found_seqs.contains(&expected_seq),
                "expected sequence {} to be finalized on Espresso, found: {:?}",
                expected_seq,
                found_seqs
            );
        }
        espresso_node.stop();
    }
}
