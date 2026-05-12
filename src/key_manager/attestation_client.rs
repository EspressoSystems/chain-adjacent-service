use anyhow::{Result, bail};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use url::Url;

use super::key_manager::AttestationVerifierClient;

#[derive(Deserialize)]
struct RawProof {
    journal: String,
}

#[derive(Deserialize)]
struct OnchainProofResponse {
    raw_proof: RawProof,
    onchain_proof: String,
}

fn strip_hex_prefix(s: &str) -> &str {
    s.strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
}

pub struct HttpAttestationVerifierClient {
    base_url: Url,
    client: Client,
}

impl HttpAttestationVerifierClient {
    pub fn new(base_url: Url, timeout_secs: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()?;
        Ok(Self { base_url, client })
    }
}

#[async_trait]
impl AttestationVerifierClient for HttpAttestationVerifierClient {
    async fn generate_zk_proof(&self, attestation: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        let url = self
            .base_url
            .join("generate_proof")
            .map_err(|e| anyhow::anyhow!("failed to construct generate_proof URL: {e}"))?;
        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/octet-stream")
            .body(attestation.to_vec())
            .send()
            .await?;

        let status = response.status();
        let response_data = response.bytes().await?;

        if !status.is_success() {
            bail!(
                "attestation service returned status {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&response_data)
            );
        }

        let zk_proof: OnchainProofResponse = serde_json::from_slice(&response_data)?;

        let journal_bytes = hex::decode(strip_hex_prefix(&zk_proof.raw_proof.journal))
            .map_err(|e| anyhow::anyhow!("failed to decode journal hex string: {e}"))?;
        let onchain_proof_bytes = hex::decode(strip_hex_prefix(&zk_proof.onchain_proof))
            .map_err(|e| anyhow::anyhow!("failed to decode onchain proof hex string: {e}"))?;

        Ok((journal_bytes, onchain_proof_bytes))
    }
}
