use alloy::{
    network::EthereumWallet,
    primitives::{Address, Bytes},
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
    sol,
};
use anyhow::Result;
use async_trait::async_trait;
use url::Url;

use super::key_manager::{EspressoTEEVerifier, TeeType};

sol! {
  #[sol(rpc)]
  interface IEspressoTEEVerifier {
      function registerService(
          bytes calldata verificationData,
          bytes calldata data,
          uint8 teeType
      ) external;

      function isSignerValid(
          address signer,
          uint8 teeType
      ) external view returns (bool);
  }
}

pub struct TEEVerifier {
    rpc_url: Url,
    contract_address: Address,
    operator_signer: PrivateKeySigner,
}

impl TEEVerifier {
    pub fn new(rpc_url: Url, contract_address: Address, operator_signer: PrivateKeySigner) -> Self {
        Self {
            rpc_url,
            contract_address,
            operator_signer,
        }
    }
}

#[async_trait]
impl EspressoTEEVerifier for TEEVerifier {
    async fn register_service(
        &self,
        verification_data: &[u8],
        data: &[u8],
        tee_type: u8,
    ) -> Result<()> {
        let wallet = EthereumWallet::from(self.operator_signer.clone());
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_http(self.rpc_url.clone());
        let contract = IEspressoTEEVerifier::new(self.contract_address, &provider);
        contract
            .registerService(
                Bytes::copy_from_slice(verification_data),
                Bytes::copy_from_slice(data),
                tee_type,
            )
            .send()
            .await?
            .watch()
            .await?;
        Ok(())
    }

    async fn registered_services(&self, addr: Address, tee_type: TeeType) -> Result<bool> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url.clone());
        let contract = IEspressoTEEVerifier::new(self.contract_address, &provider);
        let is_valid = contract.isSignerValid(addr, tee_type as u8).call().await?;
        Ok(is_valid)
    }
}
