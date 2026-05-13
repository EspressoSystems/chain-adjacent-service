use std::time::Duration;

use alloy::primitives::B256;
use base64::{Engine, engine::general_purpose};
use serde::Deserialize;

use crate::da_api::{
    error::DaApiError,
    nitro::anytrust::tree::{self},
};

const GET_BY_HASH_PATH: &str = "/get-by-hash/";

#[derive(Debug, Clone, Deserialize)]
struct RestResponse {
    #[serde(default)]
    data: String,
}

pub struct RestReader {
    client: reqwest::Client,
    rest_urls: Vec<String>,
    request_timeout: Duration,
}

impl RestReader {
    pub fn new(rest_urls: Vec<String>, request_timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            rest_urls,
            request_timeout,
        }
    }

    #[cfg(test)]
    pub fn with_http(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Try each configured REST URL in order until one returns a preimage
    /// whose `tree::valid_hash` matches the requested hash.
    pub async fn get_by_hash(&self, hash: B256) -> Result<Vec<u8>, DaApiError> {
        if self.rest_urls.is_empty() {
            return Err(DaApiError::Configuration(
                "anytrust cluster has no rest_urls configured for recovery".to_string(),
            ));
        }

        let key = hex::encode(hash.as_slice());
        let mut last_err: Option<String> = None;

        for url in &self.rest_urls {
            let full = format!("{}{GET_BY_HASH_PATH}{key}", url.trim_end_matches('/'));
            match self.try_fetch(&full, hash).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => last_err = Some(format!("{url}: {e}")),
            }
        }

        Err(DaApiError::DownstreamDa(format!(
            "all anytrust REST readers failed; last: {}",
            last_err.unwrap_or_else(|| "<unknown>".to_string())
        )))
    }

    async fn try_fetch(&self, full_url: &str, hash: B256) -> Result<Vec<u8>, DaApiError> {
        let resp = self
            .client
            .get(full_url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(|e| DaApiError::DownstreamDa(e.to_string()))?;

        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| DaApiError::ParsingError(e.to_string()))?;
        if !status.is_success() {
            return Err(DaApiError::DownstreamDa(format!(
                "status {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }

        let parsed: RestResponse = serde_json::from_slice(&bytes)
            .map_err(|e| DaApiError::ParsingError(format!("bad get-by-hash body: {e}")))?;
        let decoded = general_purpose::STANDARD
            .decode(parsed.data.as_bytes())
            .map_err(|e| DaApiError::ParsingError(format!("bad base64 in get-by-hash: {e}")))?;

        if !tree::valid_hash(hash, &decoded) {
            return Err(DaApiError::CertificateValidation(format!(
                "get-by-hash returned data whose hash does not match {hash}"
            )));
        }
        Ok(decoded)
    }
}
