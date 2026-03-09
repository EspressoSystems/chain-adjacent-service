use std::{collections::HashMap, result, sync::Arc};

use crate::da_api::{
    config::DaProviderConfig,
    da::router::DaRouter,
    error::DaApiError,
    nitro::types::{
         MaxMessageSizeResult, PreImagesResult, RecoverPayloadAndPreimagesResult,
        RecoverPayloadResult, StoreParameters, StoreResponse, SupportedHeaderBytesResult,
    },
};
use alloy::{
    primitives::{Bytes, FixedBytes},
    rpc::client,
};
use jsonrpsee::{
    core::{RpcResult, async_trait},
    proc_macros::rpc,
    types::{ErrorObject, ErrorObjectOwned},
};
use serde_json::json;
use tracing_subscriber::fmt::format::Json;

#[rpc(server, namespace = "daprovider")]
pub trait DaApi: Send + Sync {
    /// Reader methods

    #[method(name = "getSupportedHeaderBytes")]
    async fn get_supported_header_bytes(
        &self,
    ) -> RpcResult<SupportedHeaderBytesResult>;

    #[method(name = "recoverPayload")]
    async fn recover_payload(
        &self,
        batch_num: u64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Vec<u8>,
    ) -> RpcResult<RecoverPayloadResult>;

    #[method(name = "collectPreimages")]
    async fn collect_preimages(
        &self,
        batch_num: u64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Vec<u8>,
    ) -> RpcResult<PreImagesResult>;

    // #[method(name = "recoverPayloadAndPreimages")]
    // async fn recover_payload_and_preimages(
    //     &self,
    //     batch_num: u64,
    //     batch_block_hash: FixedBytes<32>,
    //     sequencer_msg: Vec<u8>,
    // ) -> RpcResult<JsonRpcResponse<RecoverPayloadAndPreimagesResult>>;

    // /// Writer methods ///

    // #[method(name = "getMaxMessageSize")]
    // async fn get_max_message_size(&self) -> RpcResult<JsonRpcResponse<MaxMessageSizeResult>>;

    // #[method(name = "store")]
    // async fn store(&self, parameters: StoreParameters)
    // -> RpcResult<JsonRpcResponse<StoreResponse>>;
}

pub struct NitroDaServer {
    router: HashMap<u8, DaProviderConfig>,
}

impl NitroDaServer {
    pub fn new(router: HashMap<u8, DaProviderConfig>) -> Self {
        Self { router }
    }

}

#[async_trait]
impl DaApiServer for NitroDaServer {
    async fn get_supported_header_bytes(&self) -> RpcResult<SupportedHeaderBytesResult> {
        let header_bytes = self.router.keys().copied().collect::<Bytes>();

        Ok(SupportedHeaderBytesResult { header_bytes })
    }

    async fn recover_payload(
        &self,
        batch_num: u64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Vec<u8>,
    ) -> RpcResult<RecoverPayloadResult> {
        if sequencer_msg.len() <= 40 {
            return Err(DaApiError::InvalidSequencerMessageLength(sequencer_msg.len()).into());
        }

        let header_byte = sequencer_msg[40];

        let da_provider_config = self
            .router
            .get(&header_byte)
            .ok_or(DaApiError::UnsupportedDaType(header_byte))?;

        let client = reqwest::Client::new();
        let request_body = json!({
            "jsonrpc": "2.0",
            "method": "daprovider_recoverPayload",
            "params": {
                "batchNum": batch_num,
                "batchBlockHash": batch_block_hash,
                "sequencerMsg": sequencer_msg
            },
            "id": 1
        });
        let result = client
            .post(&da_provider_config.endpoint_url)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?
            .json::<RecoverPayloadResult>()
            .await
            .map_err(|err| ErrorObjectOwned::from(DaApiError::Rpc(err.to_string())))?;

        Ok(result)
    }

    async fn collect_preimages(
        &self,
        batch_num: u64,
        batch_block_hash: FixedBytes<32>,
        sequencer_msg: Vec<u8>,
    ) -> RpcResult<PreImagesResult> {
        unimplemented!()
    }

    // async fn recover_payload_and_preimages(
    //     &self,
    //     batch_num: u64,
    //     batch_block_hash: FixedBytes<32>,
    //     sequencer_msg: Vec<u8>,
    // ) -> RpcResult<JsonRpcResponse<RecoverPayloadAndPreimagesResult>> {
    //     unimplemented!()
    // }
}
