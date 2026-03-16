use std::{
    collections::HashMap,
    sync::atomic::{AtomicU8, Ordering},
};

use crate::da_api::{
    certificate::nitro::CasCertificate,
    config::DaProviderConfig,
    error::DaApiError,
    nitro::{
        types::{
            GenerateCertificateValidityProofResponse, GenerateReadPreimageProofResponse,
            JsonRpcResponse, MaxMessageSizeResult, PreImagesResult,
            RecoverPayloadAndPreimagesResult, RecoverPayloadResult, StoreResponse,
            SupportedHeaderBytesResult,
        },
        utils::{
            extract_espresso_metadata_from_da_certificate,
            extract_espresso_metadata_from_sequencer_messsage,
        },
    },
};
use alloy::primitives::{Bytes, FixedBytes, U64};
use jsonrpsee::{
    core::{RpcResult, async_trait},
    proc_macros::rpc,
    types::{ ErrorObjectOwned},
};
use serde_json::json;
use tracing::info;

#[rpc(server, namespace = "daprovider")]
pub trait DaApi: Send + Sync {
    /// Reader methods

    #[method(name = "getSupportedHeaderBytes")]
    async fn get_supported_header_bytes(&self) -> RpcResult<SupportedHeaderBytesResult>;

    #[method(name = "recoverPayload")]
    async fn recover_payload(
        &self,
        batch_num: U64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Bytes,
    ) -> RpcResult<RecoverPayloadResult>;

    #[method(name = "collectPreimages")]
    async fn collect_preimages(
        &self,
        batch_num: U64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Bytes,
    ) -> RpcResult<PreImagesResult>;

    #[method(name = "recoverPayloadAndPreimages")]
    async fn recover_payload_and_preimages(
        &self,
        batch_num: U64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Bytes,
    ) -> RpcResult<RecoverPayloadAndPreimagesResult>;

    // /// Writer methods ///

    #[method(name = "getMaxMessageSize")]
    async fn get_max_message_size(&self) -> RpcResult<MaxMessageSizeResult>;

    #[method(name = "store")]
    async fn store(&self, message: Bytes, timeout: U64) -> RpcResult<StoreResponse>;

    #[method(name = "generateReadPreimageProof")]
    async fn generate_read_preimage_proof(
        &self,
        cert_hash: [u8; 32],
        offset: U64,
        certificate: Bytes,
    ) -> RpcResult<GenerateReadPreimageProofResponse>;

    #[method(name = "generateCertificateValidityProof")]
    async fn generate_certificate_validity_proof(
        &self,
        certificate: Bytes,
    ) -> RpcResult<GenerateCertificateValidityProofResponse>;
}

#[derive(Debug)]
pub struct NitroDaServer {
    ///  Strict ordering needs to be maintained in the router as the header byte is used to determine which DA provider to route the request to
    ///  Mapping (insertion index => DA provider config)
    /// Insertion 0 => ext-DA 1
    /// Insertion 1 => ext-DA 2
    router: HashMap<u8, DaProviderConfig>,
    pub current_da_provider: AtomicU8,
    pub client: reqwest::Client,
}

impl NitroDaServer {
    pub fn new(router: HashMap<u8, DaProviderConfig>) -> Self {
        Self {
            router,
            current_da_provider: AtomicU8::new(0),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl DaApiServer for NitroDaServer {
    async fn get_supported_header_bytes(&self) -> RpcResult<SupportedHeaderBytesResult> {
        info!("Received get_supported_header_bytes request");

        let da_endpoint = self
            .router
            .get(&self.current_da_provider.load(Ordering::Relaxed))
            .map(|config| config.endpoint_url.clone())
            .ok_or(DaApiError::NoDaProvidersConfigured)?;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_getSupportedHeaderBytes",
            "params": [],
            "id": 1
        });

        let result = self
            .client
            .post(&da_endpoint)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?
            .json::<JsonRpcResponse<SupportedHeaderBytesResult>>()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?;

        match result {
            JsonRpcResponse::Success { result } => Ok(result),
            JsonRpcResponse::Error { error } => Err(ErrorObjectOwned::owned(
                error.code,
                error.message,
                None::<()>,
            )),
        }
    }

    async fn recover_payload(
        &self,
        batch_num: U64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Bytes,
    ) -> RpcResult<RecoverPayloadResult> {

        info!(
            "Received recover_payload request with batch_num: {}, batch_block_hash: {:?}, sequencer_msg: {:?}",
            batch_num, batch_block_hash, sequencer_msg
        );

        if sequencer_msg.len() <= 40 {
            return Err(DaApiError::InvalidSequencerMessageLength(sequencer_msg.len()).into());
        }

        // let header_byte = Bytes::from_str(&format!("0x{:02x}", sequencer_msg[40])).unwrap();

        let da_endpoint = self
            .router
            .get(&self.current_da_provider.load(Ordering::Relaxed))
            .map(|config| config.endpoint_url.clone())
            .ok_or(DaApiError::NoDaProvidersConfigured)?;

        let da_certificate_format =
        extract_espresso_metadata_from_sequencer_messsage(&sequencer_msg).map_err(|_err| {
            ErrorObjectOwned::from(DaApiError::InvalidSequencerMessageLength(
                sequencer_msg.len(),
            ))
        })?;


        info!(
            "Extracted DA certificate format from sequencer message: {:?}",
            da_certificate_format.len()
        );

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_recoverPayload",
            "params": [

                 batch_num,
                 batch_block_hash,
                da_certificate_format
                ],
            "id": 1
        });
        let response = self
            .client
            .post(&da_endpoint)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?
            .json::<JsonRpcResponse<RecoverPayloadResult>>()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?;

        match response {
            JsonRpcResponse::Success { result } => Ok(result),
            JsonRpcResponse::Error { error } => Err(ErrorObjectOwned::owned(
                error.code,
                error.message,
                None::<()>,
            )),
        }
    }

    async fn collect_preimages(
        &self,
        batch_num: U64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Bytes,
    ) -> RpcResult<PreImagesResult> {

        if sequencer_msg.len() <= 40 {
            return Err(DaApiError::InvalidSequencerMessageLength(sequencer_msg.len()).into());
        }

        let da_endpoint = self
            .router
            .get(&self.current_da_provider.load(Ordering::Relaxed))
            .map(|config| config.endpoint_url.clone())
            .ok_or(DaApiError::NoDaProvidersConfigured)?;

        let da_certificate_format =
            extract_espresso_metadata_from_sequencer_messsage(&sequencer_msg).map_err(|_err| {
                ErrorObjectOwned::from(DaApiError::InvalidSequencerMessageLength(
                    sequencer_msg.len(),
                ))
            })?;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_collectPreimages",
            "params": [
                batch_num,
                batch_block_hash,
                da_certificate_format
                ],
            "id": 1
        });
        let result = self
            .client
            .post(&da_endpoint)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?
            .json::<JsonRpcResponse<PreImagesResult>>()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?;

        match result {
            JsonRpcResponse::Success { result } => Ok(result),
            JsonRpcResponse::Error { error } => Err(ErrorObjectOwned::owned(
                error.code,
                error.message,
                None::<()>,
            )),
        }
    }

    async fn recover_payload_and_preimages(
        &self,
        batch_num: U64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Bytes,
    ) -> RpcResult<RecoverPayloadAndPreimagesResult> {

        if sequencer_msg.len() <= 40 {
            return Err(DaApiError::InvalidSequencerMessageLength(sequencer_msg.len()).into());
        }
        // let header_byte = Bytes::from_str(&format!("0x{:02x}", sequencer_msg[40])).unwrap();

        let da_endpoint = self
            .router
            .get(&self.current_da_provider.load(Ordering::Relaxed))
            .map(|config| config.endpoint_url.clone())
            .ok_or(DaApiError::NoDaProvidersConfigured)?;

        let da_certificate_format =
            extract_espresso_metadata_from_sequencer_messsage(&sequencer_msg).map_err(|_err| {
                ErrorObjectOwned::from(DaApiError::InvalidSequencerMessageLength(
                    sequencer_msg.len(),
                ))
            })?;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_recoverPayloadAndPreimages",
            "params": [
                batch_num,
                batch_block_hash,
                da_certificate_format
                ],
            "id": 1
        });
        let result = self
            .client
            .post(&da_endpoint)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?
            .json::<JsonRpcResponse<RecoverPayloadAndPreimagesResult>>()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?;

        match result {
            JsonRpcResponse::Success { result } => Ok(result),
            JsonRpcResponse::Error { error } => Err(ErrorObjectOwned::owned(
                error.code,
                error.message,
                None::<()>,
            )),
        }
    }

    /// Writer methods ///

    async fn get_max_message_size(&self) -> RpcResult<MaxMessageSizeResult> {
        let da_endpoint = self
            .router
            .get(&self.current_da_provider.load(Ordering::Relaxed))
            .map(|config| config.endpoint_url.clone())
            .ok_or(DaApiError::NoDaProvidersConfigured)?;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_getMaxMessageSize",
            "params": [],
            "id": 1
        });

        let result = self
            .client
            .post(&da_endpoint)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?
            .json::<JsonRpcResponse<MaxMessageSizeResult>>()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?;

        match result {
            JsonRpcResponse::Success { result } => Ok(result),
            JsonRpcResponse::Error { error } => Err(ErrorObjectOwned::owned(
                error.code,
                error.message,
                None::<()>,
            )),
        }
    }

    async fn store(&self, message: Bytes, timeout: U64) -> RpcResult<StoreResponse> {
        // run CAS verification on the message
        // call DA provider store endpoint with the message and timeout
        // get certificate from DA provider...check for returned errros and handle current da provider accordingly
        // combine with espresso metadata + signature and return to caller

        info!("Received message: {}, timeout: {}", message, timeout);

        let (
            start_message_pos,
            end_message_pos,
            start_hotshot_block,
            min_hotshot_block_still_in_streamer_queue,
            batch_data,
        ) = verify_batch_data(message.clone());

        let da_endpoint = self
            .router
            .get(&self.current_da_provider.load(Ordering::Relaxed))
            .map(|config| config.endpoint_url.clone())
            .ok_or(DaApiError::NoDaProvidersConfigured)?;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_store",
            "params": [
                    message,
                    timeout
            ],
            "id": 1
        });

        let result = self
            .client
            .post(&da_endpoint)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?
            .json::<JsonRpcResponse<StoreResponse>>()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::ParsingError(err.to_string())))?;

        match result {
            JsonRpcResponse::Success { result } => {
                let final_certificate = CasCertificate::build_espresso_certificate(
                    start_message_pos,
                    end_message_pos,
                    start_hotshot_block,
                    min_hotshot_block_still_in_streamer_queue,
                    &batch_data,
                    &result.serialized_da_certificate.clone(),
                );
                //reset to primary DA provider after a successful store
                self.current_da_provider.store(0, Ordering::Relaxed);

                Ok(final_certificate.into())
            }
            JsonRpcResponse::Error { error } => {
                let error_code = error.code;
                let error_message = error.message.clone();
                match DaApiError::from(error) {
                    DaApiError::FallbackRequested(message) => {
                        // Switch to next DA provider in the router if error is a fallback request, otherwise return the error
                        if self.router.len() as u8
                            > self.current_da_provider.load(Ordering::Relaxed)
                        {
                            self.current_da_provider.store(
                                self.current_da_provider.load(Ordering::Relaxed) + 1,
                                Ordering::Relaxed,
                            );
                        } else {
                            // TODO: all DAs failed; falling back to L1!
                        }

                        Err(ErrorObjectOwned::owned(error_code, message, None::<()>))
                    }
                    _ => Err(ErrorObjectOwned::owned(
                        error_code,
                        error_message,
                        None::<()>,
                    )),
                }
            }
        }
    }

    /// VALIDATOR METHODS ///

    async fn generate_read_preimage_proof(
        &self,
        cert_hash: [u8; 32],
        offset: U64,
        certificate: Bytes,
    ) -> RpcResult<GenerateReadPreimageProofResponse> {
        // let header_byte = Bytes::from_str(&format!("0x{:02x}", certificate[150])).unwrap();

        let da_endpoint = self
            .router
            .get(&self.current_da_provider.load(Ordering::Relaxed))
            .map(|config| config.endpoint_url.clone())
            .ok_or(DaApiError::NoDaProvidersConfigured)?;

        let da_certificate_format = extract_espresso_metadata_from_da_certificate(&certificate)
            .map_err(|_err| {
                ErrorObjectOwned::from(DaApiError::InvalidSequencerMessageLength(certificate.len()))
            })?;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_generateReadPreimageProof",
            "params": [
                cert_hash,
                offset,
                da_certificate_format
                ],
            "id": 1
        });

        let result = self
            .client
            .post(&da_endpoint)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?
            .json::<JsonRpcResponse<GenerateReadPreimageProofResponse>>()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::ParsingError(err.to_string())))?;

        match result {
            JsonRpcResponse::Success { result } => Ok(result),
            JsonRpcResponse::Error { error } => {
                let error_code = error.code;
                let error_message = error.message.clone();
                match DaApiError::from(error) {
                    _ => Err(ErrorObjectOwned::owned(
                        error_code,
                        error_message,
                        None::<()>,
                    )),
                }
            }
        }
    }

    async fn generate_certificate_validity_proof(
        &self,
        certificate: Bytes,
    ) -> RpcResult<GenerateCertificateValidityProofResponse> {
        let da_endpoint = self
            .router
            .get(&self.current_da_provider.load(Ordering::Relaxed))
            .map(|config| config.endpoint_url.clone())
            .ok_or(DaApiError::NoDaProvidersConfigured)?;

        let da_certificate_format = extract_espresso_metadata_from_da_certificate(&certificate)
            .map_err(|_err| {
                ErrorObjectOwned::from(DaApiError::InvalidSequencerMessageLength(certificate.len()))
            })?;

        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_generateCertificateValidityProof",
            "params": [
                da_certificate_format
                ],
            "id": 1
        });

        let result = self
            .client
            .post(&da_endpoint)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?
            .json::<JsonRpcResponse<GenerateCertificateValidityProofResponse>>()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::ParsingError(err.to_string())))?;

        match result {
            JsonRpcResponse::Success { result } => Ok(result),
            JsonRpcResponse::Error { error } => {
                let error_code = error.code;
                let error_message = error.message.clone();
                match DaApiError::from(error) {
                    _ => Err(ErrorObjectOwned::owned(
                        error_code,
                        error_message,
                        None::<()>,
                    )),
                }
            }
        }
    }
}

// mock function
pub fn verify_batch_data(message: Bytes) -> (u32, u32, u32, u32, Vec<u8>) {
    return (0, 0, 0, 0, message.to_vec());
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Bytes, b256};
    use jsonrpsee::{
        core::{ client::ClientT},
        http_client::HttpClientBuilder,
        rpc_params,
    };
    use serde_json::{json};
    use std::{collections::HashMap, net::SocketAddr, str::FromStr};
    use tokio::{task::JoinHandle};
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use crate::da_api::{
        RollupType,
        certificate::nitro::CasCertificate,
        config::{DaApiConfig, DaProviderConfig},
        nitro::types::{RecoverPayloadResult, StoreResponse},
        run,
    };

    fn valid_message() -> Bytes {
        Bytes::from(vec![0u8; 128])
    }

    fn mock_downstream_cert_hex() -> &'static str {
        "0x010500000000000000000000" // 0x01, 0x05, then padding
    }

    fn spawn_server_with_endpoint(
        addr: SocketAddr,
        endpoint: String,
        fallback_uri: Option<String>,
    ) -> JoinHandle<()> {
        let mut da_providers = HashMap::new();
        da_providers.insert(
            0,
            DaProviderConfig {
                da_type_byte: Bytes::from_str("0x05").unwrap(),
                endpoint_url: endpoint.clone(),
                auth_token: None,
            },
        );
        da_providers.insert(
            1,
            DaProviderConfig {
                da_type_byte: Bytes::from_str("0x80").unwrap(),
                endpoint_url: fallback_uri.unwrap_or(endpoint.clone()),
                auth_token: None,
            },
        );
        let config = DaApiConfig {
            listen_addr: addr.to_string(),
            da_providers,
            ..Default::default()
        };
        tokio::spawn(async move {
            run(config, RollupType::Nitro)
                .await
                .expect("server should start");
        })
    }

    #[tokio::test]
    async fn test_mock_recover_payload_success() {
        let mock_da_provider = MockServer::start().await;

        // The mock DA provider returns a valid JSON-RPC response
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "Payload": "0x3e5aa08200000000000000000000000000000000000000000000000000000000001249c4000000000000000000000000000000000000000000000000000000000024370b000000000000000000000000e64a54e2533fd126c2e452c5fab544d80e2e4eb50000000000000000000000000000000000000000000000000000000018eab6750000000000000000000000000000000000000000000000000000000018eab845"
                }
            })))
            .mount(&mock_da_provider)
            .await;

        let addr: SocketAddr = "127.0.0.1:9945".parse().unwrap();
        let _da_server = spawn_server_with_endpoint(addr, mock_da_provider.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{}", addr))
            .unwrap();

        // sequencer_msg: 40 bytes padding + header byte 0x80 + certificate bytes
        let mut sequencer_msg = vec![0u8; 40];
        sequencer_msg.push(0x05); // header byte matching da_providers config
        sequencer_msg.extend_from_slice(b"0x200100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000d6f4495acb1e8e0c5583a2357178fffd13f0cec5b216542b40027999633d72f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ff01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1b740050eab36712d4b0f427ba6c02c9b55561fadf70a6a9cb8d1c5f801ad48f6d5b70695d5b2bf4f89cc393fdddc152fa30c2011592f27a3680eaddbf23d25455");

        let response: Result<RecoverPayloadResult, _> = client
            .request(
                "daprovider_recoverPayload",
                rpc_params![
                    80,
                    b256!("0x3e5aa082000000000000000000000000000000000000000000000000001249c4"),
                    sequencer_msg
                ],
            )
            .await;

        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_store_success_returns_cas_certificate() {
        let mock_da = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "serialized-da-cert": mock_downstream_cert_hex()
                }
            })))
            .mount(&mock_da)
            .await;

        let addr: SocketAddr = "127.0.0.1:9960".parse().unwrap();
        let _server = spawn_server_with_endpoint(addr, mock_da.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{}", addr))
            .unwrap();

        let response: Result<StoreResponse, _> = client
            .request("daprovider_store", rpc_params![valid_message(), 5000u64])
            .await;

        assert!(response.is_ok());

        let cas_cert =
            CasCertificate::try_from(response.unwrap()).expect("should convert to CasCertificate");
        // verify_batch_data returns (0,0,0,0,...) so all positions are 0
        assert_eq!(cas_cert.start_message_pos, 0);
        assert_eq!(cas_cert.end_message_pos, 0);
        assert_eq!(cas_cert.start_hotshot_block, 0);
        assert_eq!(cas_cert.min_hotshot_block_still_in_streamer_queue, 0);
        assert_eq!(cas_cert.da_api_header_flag, 0x01);
        assert_eq!(cas_cert.da_provider_flag, 0x05);
        // downstream_certificate is the raw bytes of the serialized_da_certificate
        assert!(!cas_cert.downstream_certificate.is_empty());
    }

    #[tokio::test]
    async fn test_store_malformed_response_returns_parsing_error() {
        let mock_da = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("this is not json"))
            .mount(&mock_da)
            .await;

        let addr: SocketAddr = "127.0.0.1:9963".parse().unwrap();
        let _server = spawn_server_with_endpoint(addr, mock_da.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{}", addr))
            .unwrap();

        let response: Result<StoreResponse, _> = client
            .request("daprovider_store", rpc_params![valid_message(), 5000u64])
            .await;

        assert!(response.is_err());
        let err = response.unwrap_err().to_string();
        assert!(
            err.contains("ParsingError") || err.contains("parsing error"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_store_wrong_field_name_in_response_fails_parsing() {
        let mock_da = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    // wrong key — "serializedDaCertificate" instead of "serialized-da-cert"
                    "serializedDaCertificate": mock_downstream_cert_hex()
                }
            })))
            .mount(&mock_da)
            .await;

        let addr: SocketAddr = "127.0.0.1:9964".parse().unwrap();
        let _server = spawn_server_with_endpoint(addr, mock_da.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{}", addr))
            .unwrap();

        let response: Result<StoreResponse, _> = client
            .request("daprovider_store", rpc_params![valid_message(), 5000u64])
            .await;

        assert!(response.is_err(), "should fail with wrong field name");
    }

    #[tokio::test]
    async fn test_store_da_provider_generic_error_propagates() {
        let mock_da = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32000,
                    "message": "storage backend unavailable"
                }
            })))
            .mount(&mock_da)
            .await;

        let addr: SocketAddr = "127.0.0.1:9965".parse().unwrap();
        let _server = spawn_server_with_endpoint(addr, mock_da.uri(), None);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{}", addr))
            .unwrap();

        let response: Result<StoreResponse, _> = client
            .request("daprovider_store", rpc_params![valid_message(), 5000u64])
            .await;

        assert!(response.is_err());
        let err = response.unwrap_err().to_string();
        assert!(
            err.contains("storage backend unavailable"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_store_fallback_error_switches_to_next_provider() {
        let primary_mock = MockServer::start().await;
        let fallback_mock = MockServer::start().await;

        // Primary returns FallbackRequested
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32001, // TODO: replace with the actual FallbackRequested code from DaApiError
                    "message": "DA provider requests fallback to next writer: storage temporarily unavailable"
                }
            })))
            .mount(&primary_mock)
            .await;

        // Fallback returns success
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "serialized-da-cert": mock_downstream_cert_hex()
                }
            })))
            .mount(&fallback_mock)
            .await;

        let addr: SocketAddr = "127.0.0.1:9966".parse().unwrap();
        let _server =
            spawn_server_with_endpoint(addr, primary_mock.uri(), Some(fallback_mock.uri()));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = HttpClientBuilder::default()
            .build(format!("http://{}", addr))
            .unwrap();

        // First call hits primary → gets FallbackRequested → increments provider index
        let first: Result<CasCertificate, _> = client
            .request("daprovider_store", rpc_params![valid_message(), 5000u64])
            .await;
        assert!(
            first.is_err(),
            "first call should return the fallback error to caller"
        );

        // Second call should now hit the fallback provider
        // NOTE: remove ignore once build_and_sign_payload is implemented
        let second: Result<StoreResponse, _> = client
            .request("daprovider_store", rpc_params![valid_message(), 5000u64])
            .await;
        assert!(
            second.is_ok(),
            "second call should succeed via fallback provider"
        );

        // Verify exactly 1 request hit the primary, 0 hit the fallback (for now)
        let primary_hits = primary_mock.received_requests().await.unwrap().len();
        assert_eq!(
            primary_hits, 1,
            "primary should have received exactly 1 request"
        );
    }
}

