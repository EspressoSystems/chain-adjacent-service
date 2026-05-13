use std::time::Duration;

use alloy::primitives::B256;
use futures::{StreamExt, stream::FuturesUnordered};
use tracing::warn;

use crate::da_api::{
    error::DaApiError,
    nitro::anytrust::{
        bls::{RawPublicKey, SIG_BYTES, aggregate_signatures},
        cert::{DataAvailabilityCertificate, keyset_hash_and_bytes},
        client::{StoreResult, das_store},
        config::AnytrustClusterConfig,
        tree,
    },
};

const CERT_VERSION: u8 = 1;

pub struct AnytrustAggregator {
    cluster_name: String,
    backends: Vec<Backend>,
    request_timeout: Duration,
    required_successes: usize,
    keyset_hash: B256,
    http: reqwest::Client,
}

struct Backend {
    url: String,
    signers_mask: u64,
}

impl AnytrustAggregator {
    pub fn from_config(
        cluster_name: String,
        cfg: AnytrustClusterConfig,
        http: reqwest::Client,
    ) -> Result<Self, DaApiError> {
        if cfg.backends.is_empty() {
            return Err(DaApiError::Configuration(format!(
                "anytrust cluster {cluster_name}: no backends configured"
            )));
        }
        if cfg.backends.len() > 64 {
            return Err(DaApiError::Configuration(format!(
                "anytrust cluster {cluster_name}: at most 64 backends supported, got {}",
                cfg.backends.len()
            )));
        }
        let n = cfg.backends.len();
        let h = cfg.assumed_honest as usize;
        if h == 0 || h > n {
            return Err(DaApiError::Configuration(format!(
                "anytrust cluster {cluster_name}: assumed_honest must be in 1..={n}, got {h}"
            )));
        }
        let required_successes = n + 1 - h;

        let raw_pubkeys: Vec<Vec<u8>> = cfg
            .backends
            .iter()
            .map(|b| RawPublicKey::from_base64(&b.pubkey).map(|p| p.0))
            .collect::<Result<_, _>>()?;
        let (keyset_hash, _keyset_bytes) =
            keyset_hash_and_bytes(cfg.assumed_honest as u64, &raw_pubkeys)?;

        let backends: Vec<Backend> = cfg
            .backends
            .into_iter()
            .enumerate()
            .map(|(i, b)| Backend {
                url: b.url,
                signers_mask: 1u64 << i,
            })
            .collect();

        Ok(Self {
            cluster_name,
            backends,
            request_timeout: Duration::from_millis(cfg.request_timeout_ms),
            required_successes,
            keyset_hash,
            http,
        })
    }

    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    /// Aggregate-store the message. Returns the serialized AnyTrust
    /// `DataAvailabilityCertificate` bytes (this is what gets wrapped in the
    /// outer CAS certificate before being returned to the batch poster).
    pub async fn store(&self, message: &[u8], timeout: u64) -> Result<Vec<u8>, DaApiError> {
        let expected_hash = tree::hash(message);

        let mut tasks: FuturesUnordered<_> = self
            .backends
            .iter()
            .map(|b| async move {
                let res =
                    das_store(&self.http, &b.url, message, timeout, self.request_timeout).await;
                (b.signers_mask, b.url.as_str(), res)
            })
            .collect();

        let mut sigs: Vec<[u8; SIG_BYTES]> = Vec::with_capacity(self.backends.len());
        let mut signers_mask: u64 = 0;
        let mut failures: usize = 0;
        let max_failures = self.backends.len().saturating_sub(self.required_successes);

        while let Some((mask, url, result)) = tasks.next().await {
            match validate_response(
                result,
                expected_hash.as_slice(),
                timeout,
                self.keyset_hash.as_slice(),
            ) {
                Ok(sig) => {
                    sigs.push(sig);
                    signers_mask |= mask;
                    if sigs.len() >= self.required_successes {
                        break;
                    }
                }
                Err(e) => {
                    warn!(cluster = %self.cluster_name, backend = %url, err = %e, "anytrust backend store failed");
                    failures += 1;
                    if failures > max_failures {
                        return Err(DaApiError::DownstreamDa(format!(
                            "anytrust cluster {}: too many backend failures ({failures}/{}); need {} successes",
                            self.cluster_name,
                            self.backends.len(),
                            self.required_successes,
                        )));
                    }
                }
            }
        }

        if sigs.len() < self.required_successes {
            return Err(DaApiError::DownstreamDa(format!(
                "anytrust cluster {}: only got {} successes, need {}",
                self.cluster_name,
                sigs.len(),
                self.required_successes,
            )));
        }

        let aggregated_sig = aggregate_signatures(&sigs)?;

        let cert = DataAvailabilityCertificate {
            keyset_hash: self.keyset_hash,
            data_hash: expected_hash,
            timeout,
            version: CERT_VERSION,
            signers_mask,
            sig: aggregated_sig,
        };
        Ok(cert.serialize())
    }
}

fn validate_response(
    result: Result<StoreResult, DaApiError>,
    expected_hash: &[u8],
    expected_timeout: u64,
    expected_keyset_hash: &[u8],
) -> Result<[u8; SIG_BYTES], DaApiError> {
    let r = result?;
    if r.data_hash.as_slice() != expected_hash {
        return Err(DaApiError::DownstreamDa(
            "backend returned mismatched dataHash".to_string(),
        ));
    }
    if r.timeout != expected_timeout {
        return Err(DaApiError::DownstreamDa(format!(
            "backend returned timeout {} expected {expected_timeout}",
            r.timeout
        )));
    }
    // Reject backends signing under a different keyset than the one the
    // cluster is configured with — otherwise we'd aggregate signatures the
    // L1 reader can never verify against our keyset_hash.
    if r.keyset_hash.as_slice() != expected_keyset_hash {
        return Err(DaApiError::DownstreamDa(format!(
            "backend returned keysetHash 0x{} expected 0x{}",
            hex::encode(r.keyset_hash),
            hex::encode(expected_keyset_hash)
        )));
    }
    Ok(r.sig)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::{Json, Router, routing::post};
    use base64::{Engine, engine::general_purpose};
    use blst::min_sig::SecretKey;
    use serde_json::{Value, json};

    use crate::da_api::nitro::anytrust::{
        config::{AnytrustClusterConfig, BackendConfig},
        tree,
    };

    use super::*;

    struct MockBackend {
        url: String,
    }

    /// Spin up a tiny axum-based mock backend that returns a blst-signed
    /// `das_store` response containing the given `data_hash`/`timeout`. The
    /// signature is computed with a fresh per-backend secret key; CAS doesn't
    /// verify it, so we just need a 96-byte G1 point on the wire.
    ///
    /// Match `mismatch_hash` to make the backend return the wrong data hash
    /// (to exercise the aggregator's quorum-failure path).
    async fn spawn_backend(
        data_hash: [u8; 32],
        timeout: u64,
        keyset_hash: [u8; 32],
        mismatch_hash: bool,
    ) -> MockBackend {
        let seed = alloy::primitives::keccak256(
            format!("{data_hash:?}{timeout}{keyset_hash:?}{mismatch_hash}").as_bytes(),
        );
        let mut ikm = [0u8; 32];
        ikm.copy_from_slice(seed.as_slice());
        let sk = SecretKey::key_gen(&ikm, &[]).unwrap();
        let sig = sk
            .sign(&data_hash, b"DST_FOR_BLST_TEST_AGG", &[])
            .serialize();
        let reported_hash = if mismatch_hash { [0u8; 32] } else { data_hash };

        let response = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "dataHash": format!("0x{}", hex::encode(reported_hash)),
                "timeout": format!("0x{:x}", timeout),
                "signersMask": "0x0",
                "keysetHash": format!("0x{}", hex::encode(keyset_hash)),
                "sig": format!("0x{}", hex::encode(sig)),
                "version": "0x1",
            }
        });
        let response_clone = response.clone();

        let app = Router::new().route(
            "/",
            post(move |Json(_body): Json<Value>| {
                let resp = response_clone.clone();
                async move { Json(resp) }
            }),
        );

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound = listener.local_addr().unwrap();
        let ready_tx = tokio::sync::oneshot::channel::<()>();
        let (tx, rx) = (ready_tx.0, ready_tx.1);
        tokio::spawn(async move {
            // Signal readiness before starting to serve; serve() runs forever.
            let _ = tx.send(());
            axum::serve(listener, app).await.unwrap();
        });
        let _ = rx.await;
        // Yield to give the spawned task a chance to enter axum::serve.
        tokio::task::yield_now().await;

        MockBackend {
            url: format!("http://{bound}"),
        }
    }

    fn synthetic_pubkey_bytes(seed: u8) -> Vec<u8> {
        // 1 byte proof_len + 96 bytes proof + 192 bytes key, total 289.
        // The contents don't matter for CAS's aggregator since we don't
        // verify per-backend signatures.
        let mut out = vec![seed; 289];
        out[0] = 96;
        out
    }

    #[tokio::test]
    async fn aggregator_collects_quorum_and_builds_cert() {
        let message = b"hello anytrust world";
        let timeout: u64 = 12345;
        let data_hash: [u8; 32] = tree::hash(message).into();

        // 3-backend cluster, AssumedHonest = 2 ⇒ need K = 2 successes.
        let pubkeys = [
            synthetic_pubkey_bytes(0xa1),
            synthetic_pubkey_bytes(0xb2),
            synthetic_pubkey_bytes(0xc3),
        ];
        let (expected_keyset_hash, _) =
            crate::da_api::nitro::anytrust::cert::keyset_hash_and_bytes(2, pubkeys.as_ref())
                .unwrap();

        let b0 = spawn_backend(data_hash, timeout, expected_keyset_hash.into(), false).await;
        let b1 = spawn_backend(data_hash, timeout, expected_keyset_hash.into(), false).await;
        let b2 = spawn_backend(data_hash, timeout, expected_keyset_hash.into(), false).await;

        let cfg = AnytrustClusterConfig {
            backends: vec![
                BackendConfig {
                    url: b0.url.clone(),
                    pubkey: general_purpose::STANDARD.encode(&pubkeys[0]),
                },
                BackendConfig {
                    url: b1.url.clone(),
                    pubkey: general_purpose::STANDARD.encode(&pubkeys[1]),
                },
                BackendConfig {
                    url: b2.url.clone(),
                    pubkey: general_purpose::STANDARD.encode(&pubkeys[2]),
                },
            ],
            rest_urls: vec![],
            assumed_honest: 2,
            request_timeout_ms: 5_000,
        };
        let aggregator = AnytrustAggregator::from_config(
            "test".to_string(),
            cfg,
            reqwest::Client::builder().no_proxy().build().unwrap(),
        )
        .expect("from_config");

        let cert_bytes = aggregator.store(message, timeout).await.expect("store");

        // Cert layout: 1 + 32 + 32 + 8 + 1 (v1) + 8 + 96 = 178 bytes.
        assert_eq!(cert_bytes.len(), 1 + 32 + 32 + 8 + 1 + 8 + 96);
        assert_eq!(cert_bytes[0], 0x80 | 0x08);
        assert_eq!(&cert_bytes[1..33], expected_keyset_hash.as_slice());
        assert_eq!(&cert_bytes[33..65], &data_hash);
        assert_eq!(&cert_bytes[65..73], &timeout.to_be_bytes());
        assert_eq!(cert_bytes[73], 1);
    }

    #[tokio::test]
    async fn aggregator_fails_when_quorum_not_reached() {
        let message = b"insufficient quorum";
        let timeout: u64 = 555;
        let data_hash: [u8; 32] = tree::hash(message).into();

        // 2-backend cluster, AssumedHonest = 1 ⇒ need K = 2. One backend
        // returns wrong dataHash, the other is fine — we should still fail.
        let pubkeys = [synthetic_pubkey_bytes(0x11), synthetic_pubkey_bytes(0x22)];
        let (ks_hash, _) =
            crate::da_api::nitro::anytrust::cert::keyset_hash_and_bytes(1, pubkeys.as_ref())
                .unwrap();

        let good = spawn_backend(data_hash, timeout, ks_hash.into(), false).await;
        let bad = spawn_backend(data_hash, timeout, ks_hash.into(), true).await;

        let cfg = AnytrustClusterConfig {
            backends: vec![
                BackendConfig {
                    url: good.url.clone(),
                    pubkey: general_purpose::STANDARD.encode(&pubkeys[0]),
                },
                BackendConfig {
                    url: bad.url.clone(),
                    pubkey: general_purpose::STANDARD.encode(&pubkeys[1]),
                },
            ],
            rest_urls: vec![],
            assumed_honest: 1,
            request_timeout_ms: 5_000,
        };
        let aggregator = AnytrustAggregator::from_config(
            "test".to_string(),
            cfg,
            reqwest::Client::builder().no_proxy().build().unwrap(),
        )
        .expect("from_config");

        let err = aggregator.store(message, timeout).await.unwrap_err();
        assert!(matches!(err, DaApiError::DownstreamDa(_)), "got: {err}");
    }
}
