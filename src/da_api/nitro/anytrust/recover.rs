use alloy::primitives::{B256, keccak256};

use crate::da_api::{
    error::DaApiError,
    nitro::{
        anytrust::{
            bls::RawPublicKey,
            cert::{DataAvailabilityCertificate, keyset_hash_and_bytes},
            config::AnytrustClusterConfig,
            reader::RestReader,
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
    /// Pre-computed keyset for this cluster — `(hash, serialized_bytes)`.
    /// Upstream's `KeysetFetcher` reads these from L1 `SetValidKeyset` events
    /// (daprovider/anytrust/keyset_fetcher.go); daserver REST endpoints only
    /// store payloads, not keysets, so fetching the keyset from REST would
    /// always 404. We re-derive the keyset locally from the cluster config —
    /// `(assumed_honest, pubkeys)` already pins it byte-for-byte.
    keyset_hash: B256,
    keyset_bytes: Vec<u8>,
}

impl AnytrustRecovery {
    pub fn from_config(
        cfg: &AnytrustClusterConfig,
        http: reqwest::Client,
    ) -> Result<Self, DaApiError> {
        let raw_pubkeys: Vec<Vec<u8>> = cfg
            .backends
            .iter()
            .map(|b| RawPublicKey::from_base64(&b.pubkey).map(|p| p.0))
            .collect::<Result<_, _>>()?;
        let (keyset_hash, keyset_bytes) =
            keyset_hash_and_bytes(cfg.assumed_honest as u64, &raw_pubkeys)?;
        Ok(Self {
            reader: RestReader::new(
                http,
                cfg.rest_urls.clone(),
                std::time::Duration::from_millis(cfg.request_timeout_ms),
            ),
            keyset_hash,
            keyset_bytes,
        })
    }

    #[cfg(test)]
    pub fn from_reader_with_keyset(
        reader: RestReader,
        keyset_hash: B256,
        keyset_bytes: Vec<u8>,
    ) -> Self {
        Self {
            reader,
            keyset_hash,
            keyset_bytes,
        }
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

        // Keyset: derive locally from cluster config (see field doc on
        // `keyset_bytes`). Reject certs that reference a different keyset
        // than the one we've been told about — they're outside this
        // cluster's purview and we have no way to validate them.
        if cert.keyset_hash != self.keyset_hash {
            return Err(DaApiError::CertificateValidation(format!(
                "anytrust cert references unknown keyset 0x{} (cluster keyset is 0x{})",
                hex::encode(cert.keyset_hash),
                hex::encode(self.keyset_hash),
            )));
        }
        if let Some(ref mut p) = preimages {
            tree::record_hash(&self.keyset_bytes, |h, d| {
                p.push(Preimage {
                    hash: h,
                    data: d.to_vec(),
                })
            });
        }

        let mut payload: Option<Vec<u8>> = None;
        if want_payload || want_preimages {
            // For v0 certs the cert holds a flat keccak hash, but daservers
            // store the payload under `FlatHashToTreeHash(flat)` in the
            // tree-style layout. Try the tree-rewritten key first, fall
            // back to the flat one (mirrors upstream
            // recoverPayloadFromBatchInternal's getByHash closure in
            // daprovider/anytrust/util/util.go).
            let data = if cert.version == 0 {
                let tree_key = tree::flat_hash_to_tree_hash(cert.data_hash);
                match self.reader.get_by_hash(tree_key).await {
                    Ok(d) => d,
                    Err(_) => self.reader.get_by_hash(cert.data_hash).await?,
                }
            } else {
                self.reader.get_by_hash(cert.data_hash).await?
            };

            // Version-specific integrity check: v0 commits to the flat
            // keccak of the payload; v1 commits to the tree hash.
            // `RestReader::get_by_hash` already validates via
            // `tree::valid_hash`, but that accepts either form when the
            // preimage isn't a tree node — so for v0 we re-check explicitly
            // against the cert's flat hash.
            if cert.version == 0 && keccak256(&data) != cert.data_hash {
                return Err(DaApiError::CertificateValidation(format!(
                    "v0 cert: keccak256(payload) does not match cert.data_hash 0x{}",
                    hex::encode(cert.data_hash),
                )));
            }

            if let Some(ref mut p) = preimages {
                if cert.version == 0 {
                    // v0 records two entries: the flat dataHash → payload,
                    // plus keccak256(tree_leaf) → tree_leaf so validators
                    // that walk the tree-style index can still resolve it
                    // (matches util.go:283-287).
                    p.push(Preimage {
                        hash: cert.data_hash,
                        data: data.clone(),
                    });
                    let tree_leaf = tree::flat_hash_to_tree_leaf(cert.data_hash);
                    p.push(Preimage {
                        hash: keccak256(&tree_leaf),
                        data: tree_leaf,
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

        let reader = RestReader::new(
            reqwest::Client::builder().no_proxy().build().unwrap(),
            vec![rest.url.clone()],
            Duration::from_secs(5),
        );
        let recovery = AnytrustRecovery::from_reader_with_keyset(
            reader,
            cert.keyset_hash,
            keyset_bytes.clone(),
        );

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

        let reader = RestReader::new(
            reqwest::Client::builder().no_proxy().build().unwrap(),
            vec![rest.url.clone()],
            Duration::from_secs(5),
        );
        let recovery = AnytrustRecovery::from_reader_with_keyset(
            reader,
            cert.keyset_hash,
            keyset_bytes.clone(),
        );

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

        let reader = RestReader::new(
            reqwest::Client::builder().no_proxy().build().unwrap(),
            vec![rest.url.clone()],
            Duration::from_secs(5),
        );
        let recovery = AnytrustRecovery::from_reader_with_keyset(
            reader,
            cert.keyset_hash,
            keyset_bytes.clone(),
        );

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

    /// The recovery doesn't fetch the keyset from REST anymore (upstream
    /// daservers don't serve it), so a recovery configured with a
    /// different keyset than the cert references must reject locally.
    #[tokio::test]
    async fn recover_rejects_unknown_keyset() {
        let payload = b"payload".to_vec();
        let (cert, _, _) = make_cert(&payload, 1);

        // Mock REST has the payload available, but recovery is configured
        // with a *different* keyset (all zeros). The recovery should reject
        // before even hitting REST.
        let rest = spawn_rest(vec![(cert.data_hash, payload.clone())]).await;
        let reader = RestReader::new(
            reqwest::Client::builder().no_proxy().build().unwrap(),
            vec![rest.url.clone()],
            Duration::from_secs(5),
        );
        let other_keyset_hash = B256::repeat_byte(0x55);
        let recovery =
            AnytrustRecovery::from_reader_with_keyset(reader, other_keyset_hash, vec![0u8; 1]);

        let mut seq_msg = vec![0u8; SEQUENCER_HEADER_LEN];
        seq_msg.extend_from_slice(&cert.serialize());

        let err = recovery.recover_payload(&seq_msg).await.unwrap_err();
        match &err {
            DaApiError::CertificateValidation(msg) => {
                assert!(msg.contains("unknown keyset"), "{msg}")
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    /// V0 certs commit to a flat keccak hash, but storage backends key data
    /// by the tree-rewritten hash. Recovery must try the tree key first and
    /// fall back to the flat one, matching upstream
    /// `recoverPayloadFromBatchInternal` (util.go:218-244).
    #[tokio::test]
    async fn recover_v0_payload_via_tree_rewritten_key() {
        use alloy::primitives::keccak256;

        let payload = b"v0 payload bytes".to_vec();
        let pubkeys = vec![vec![0xaa; 289]];
        let (keyset_hash, keyset_bytes) = keyset_hash_and_bytes(1, &pubkeys).unwrap();
        let flat_data_hash = keccak256(&payload);
        let cert = DataAvailabilityCertificate {
            keyset_hash,
            data_hash: flat_data_hash,
            timeout: 999,
            version: 0,
            signers_mask: 0b1,
            sig: [0u8; SIG_BYTES],
        };

        // Daserver behavior: payload is keyed by the *tree-rewritten* hash,
        // not the flat one. The recovery must rewrite-then-fall-back to find it.
        let tree_key = crate::da_api::nitro::anytrust::tree::flat_hash_to_tree_hash(flat_data_hash);
        let rest = spawn_rest(vec![(tree_key, payload.clone())]).await;
        let reader = RestReader::new(
            reqwest::Client::builder().no_proxy().build().unwrap(),
            vec![rest.url.clone()],
            Duration::from_secs(5),
        );
        let recovery = AnytrustRecovery::from_reader_with_keyset(
            reader,
            cert.keyset_hash,
            keyset_bytes.clone(),
        );

        let mut seq_msg = vec![0u8; SEQUENCER_HEADER_LEN];
        seq_msg.extend_from_slice(&cert.serialize());

        let got = recovery.recover_payload(&seq_msg).await.expect("recover");
        assert_eq!(got, payload);
    }
}
