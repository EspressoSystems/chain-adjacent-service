use std::time::Duration;
use tokio::time::sleep;

/// Implements an exponential backoff strategy for retrying operations.
pub async fn exponential_backoff(current: Duration, max: Duration) -> Duration {
    sleep(current).await;
    std::cmp::min(current.saturating_mul(2), max)
}
