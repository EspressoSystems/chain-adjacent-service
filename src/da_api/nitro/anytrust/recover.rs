use alloy::primitives::B256;

use crate::da_api::{
    error::DaApiError,
    nitro::{
        anytrust::{
            cert::DataAvailabilityCertificate, config::AnytrustClusterConfig, reader::RestReader,
            tree,
        },
        utils::SEQUENCER_HEADER_LEN,
    },
};

/// One Keccak256 preimage that the tree walk produced (used to build the
/// `Preimages` map in the `daprovider_collectPreimages` response).
pub struct Preimage {
    pub hash: B256,
    pub data: Vec<u8>,
}

pub struct AnytrustRecovery {
    reader: RestReader,
}

impl AnytrustRecovery {
    pub fn from_config(cfg: &AnytrustClusterConfig) -> Self {
        Self {
            reader: RestReader::new(
                cfg.rest_urls.clone(),
                std::time::Duration::from_millis(cfg.request_timeout_ms),
            ),
        }
    }

    #[cfg(test)]
    pub fn from_reader(reader: RestReader) -> Self {
        Self { reader }
    }

    /// Recover the payload. Returns the inner batch bytes.
    pub async fn recover_payload(&self, sequencer_msg: &[u8]) -> Result<Vec<u8>, DaApiError> {
        let (_, payload, _) = self.recover(sequencer_msg, true, false).await?;
        Ok(payload.expect("recover requested payload but got none"))
    }

    /// Collect preimages without returning the payload. We still need to fetch
    /// it though, because the keccak256 preimages we record include the
    /// payload's tree leaves.
    pub async fn collect_preimages(
        &self,
        sequencer_msg: &[u8],
    ) -> Result<Vec<Preimage>, DaApiError> {
        let (_, _, preimages) = self.recover(sequencer_msg, false, true).await?;
        Ok(preimages.unwrap_or_default())
    }

    /// Both payload and preimages.
    pub async fn recover_payload_and_preimages(
        &self,
        sequencer_msg: &[u8],
    ) -> Result<(Vec<u8>, Vec<Preimage>), DaApiError> {
        let (_, payload, preimages) = self.recover(sequencer_msg, true, true).await?;
        Ok((
            payload.expect("recover requested payload but got none"),
            preimages.unwrap_or_default(),
        ))
    }

    async fn recover(
        &self,
        sequencer_msg: &[u8],
        want_payload: bool,
        want_preimages: bool,
    ) -> Result<
        (
            DataAvailabilityCertificate,
            Option<Vec<u8>>,
            Option<Vec<Preimage>>,
        ),
        DaApiError,
    > {
        if sequencer_msg.len() <= SEQUENCER_HEADER_LEN {
            return Err(DaApiError::InvalidSequencerMessageLength(
                SEQUENCER_HEADER_LEN,
                sequencer_msg.len(),
            ));
        }

        let cert =
            DataAvailabilityCertificate::deserialize(&sequencer_msg[SEQUENCER_HEADER_LEN..])?;
        if cert.version >= 2 {
            return Err(DaApiError::CertificateValidation(format!(
                "anytrust cert version {} not supported",
                cert.version
            )));
        }

        let mut preimages: Option<Vec<Preimage>> = if want_preimages {
            Some(Vec::new())
        } else {
            None
        };

        // Always need the keyset preimage; record it if we're collecting.
        let keyset_bytes = self.reader.get_by_hash(cert.keyset_hash).await?;
        if let Some(ref mut p) = preimages {
            tree::record_hash(&keyset_bytes, |h, d| {
                p.push(Preimage {
                    hash: h,
                    data: d.to_vec(),
                })
            });
        }

        let mut payload: Option<Vec<u8>> = None;
        if want_payload || want_preimages {
            let data = self.reader.get_by_hash(cert.data_hash).await?;
            if let Some(ref mut p) = preimages {
                if cert.version == 0 {
                    // v0: flat keccak. Upstream also records the synthetic
                    // tree leaf so a v0 cert can be re-hashed by validators
                    // that expect tree-style preimages.
                    p.push(Preimage {
                        hash: cert.data_hash,
                        data: data.clone(),
                    });
                } else {
                    tree::record_hash(&data, |h, d| {
                        p.push(Preimage {
                            hash: h,
                            data: d.to_vec(),
                        })
                    });
                }
            }
            if want_payload {
                payload = Some(data);
            }
        }

        Ok((cert, payload, preimages))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use axum::{Router, extract::Path, http::StatusCode, response::IntoResponse, routing::get};
    use base64::{Engine, engine::general_purpose};

    use crate::da_api::nitro::{
        anytrust::{
            bls::SIG_BYTES,
            cert::{DataAvailabilityCertificate, keyset_hash_and_bytes},
        },
        utils::SEQUENCER_HEADER_LEN,
    };

    use super::*;

    struct MockRestServer {
        url: String,
    }

    /// Tiny axum mock that serves `/get-by-hash/<hex-key>` for the provided
    /// `(hash → bytes)` table.
    async fn spawn_rest(items: Vec<(B256, Vec<u8>)>) -> MockRestServer {
        let table: std::sync::Arc<std::collections::HashMap<String, Vec<u8>>> = std::sync::Arc::new(
            items
                .into_iter()
                .map(|(h, v)| (hex::encode(h.as_slice()), v))
                .collect(),
        );

        let app = Router::new().route(
            "/get-by-hash/{key}",
            get(move |Path(key): Path<String>| {
                let table = table.clone();
                async move {
                    match table.get(&key) {
                        Some(v) => {
                            let body = serde_json::json!({
                                "data": general_purpose::STANDARD.encode(v),
                            });
                            (StatusCode::OK, axum::Json(body)).into_response()
                        }
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }
            }),
        );

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = tx.send(());
            axum::serve(listener, app).await.unwrap();
        });
        let _ = rx.await;
        tokio::task::yield_now().await;

        MockRestServer {
            url: format!("http://{bound}"),
        }
    }

    fn make_cert(
        payload: &[u8],
        assumed_honest: u64,
    ) -> (DataAvailabilityCertificate, Vec<u8>, Vec<u8>) {
        let pubkeys = vec![vec![0xaa; 289], vec![0xbb; 289]];
        let (keyset_hash, keyset_bytes) = keyset_hash_and_bytes(assumed_honest, &pubkeys).unwrap();
        let data_hash = crate::da_api::nitro::anytrust::tree::hash(payload);
        let cert = DataAvailabilityCertificate {
            keyset_hash,
            data_hash,
            timeout: 999,
            version: 1,
            signers_mask: 0b11,
            sig: [0u8; SIG_BYTES],
        };
        (cert, keyset_bytes, payload.to_vec())
    }

    #[tokio::test]
    async fn recover_payload_round_trip() {
        let payload = b"the inner batch payload that anytrust stored".to_vec();
        let (cert, keyset_bytes, _) = make_cert(&payload, 1);

        let rest = spawn_rest(vec![
            (cert.keyset_hash, keyset_bytes.clone()),
            (cert.data_hash, payload.clone()),
        ])
        .await;

        let reader = RestReader::new(vec![rest.url.clone()], Duration::from_secs(5))
            .with_http(reqwest::Client::builder().no_proxy().build().unwrap());
        let recovery = AnytrustRecovery::from_reader(reader);

        let mut seq_msg = vec![0u8; SEQUENCER_HEADER_LEN];
        seq_msg.extend_from_slice(&cert.serialize());

        let got = recovery.recover_payload(&seq_msg).await.expect("recover");
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn recover_payload_and_preimages_includes_data_and_keyset_hashes() {
        let payload = b"abcdefg".to_vec();
        let (cert, keyset_bytes, _) = make_cert(&payload, 1);

        let rest = spawn_rest(vec![
            (cert.keyset_hash, keyset_bytes.clone()),
            (cert.data_hash, payload.clone()),
        ])
        .await;

        let reader = RestReader::new(vec![rest.url.clone()], Duration::from_secs(5))
            .with_http(reqwest::Client::builder().no_proxy().build().unwrap());
        let recovery = AnytrustRecovery::from_reader(reader);

        let mut seq_msg = vec![0u8; SEQUENCER_HEADER_LEN];
        seq_msg.extend_from_slice(&cert.serialize());

        let (got_payload, preimages) = recovery
            .recover_payload_and_preimages(&seq_msg)
            .await
            .expect("recover");
        assert_eq!(got_payload, payload);

        // The recorded preimages must let a consumer recompute both the
        // keyset hash and the data hash via tree::record_hash semantics —
        // i.e. the raw keyset_bytes and payload should appear as one of the
        // recorded preimages (as the deepest bin-level entries).
        let hashes: Vec<_> = preimages.iter().map(|p| (p.hash, p.data.clone())).collect();
        assert!(
            hashes.iter().any(|(_, d)| d == &keyset_bytes),
            "keyset bytes should appear in preimages"
        );
        assert!(
            hashes.iter().any(|(_, d)| d == &payload),
            "payload bytes should appear in preimages"
        );
    }

    #[tokio::test]
    async fn recover_rejects_mismatched_data() {
        let payload = b"real payload".to_vec();
        let (cert, keyset_bytes, _) = make_cert(&payload, 1);

        // REST returns a *different* payload under the data hash key.
        let rest = spawn_rest(vec![
            (cert.keyset_hash, keyset_bytes.clone()),
            (cert.data_hash, b"tampered".to_vec()),
        ])
        .await;

        let reader = RestReader::new(vec![rest.url.clone()], Duration::from_secs(5))
            .with_http(reqwest::Client::builder().no_proxy().build().unwrap());
        let recovery = AnytrustRecovery::from_reader(reader);

        let mut seq_msg = vec![0u8; SEQUENCER_HEADER_LEN];
        seq_msg.extend_from_slice(&cert.serialize());

        let err = recovery.recover_payload(&seq_msg).await.unwrap_err();
        // RestReader::try_fetch raises CertificateValidation on mismatch, but
        // since we fall back through every URL the final error is DownstreamDa.
        match &err {
            DaApiError::DownstreamDa(msg) => assert!(msg.contains("does not match"), "{msg}"),
            other => panic!("unexpected error: {other}"),
        }
    }
}
