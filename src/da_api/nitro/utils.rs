use alloy::primitives::{Address, Bytes};

use crate::da_api::{error::DaApiError, nitro::certificate::CasCertificate};

pub const SEQUENCER_HEADER_LEN: usize = 40;

/// Sequencer message format is: [SequencerHeader(40 bytes), EspressoCert(117 bytes), DACert]
pub fn try_extract_da_sequencer_msg_from_espresso_da_cert(
    sequencer_msg: &Bytes,
    expected_signer: Address,
    parent_chain_id: u64,
    tee_verifier_address: Address,
) -> Result<Bytes, DaApiError> {
    if sequencer_msg.len() <= SEQUENCER_HEADER_LEN {
        return Err(DaApiError::InvalidSequencerMessageLength(
            SEQUENCER_HEADER_LEN,
            sequencer_msg.len(),
        ));
    }

    let cas_cert = CasCertificate::from_bytes(&sequencer_msg[SEQUENCER_HEADER_LEN..])?;
    cas_cert.validate(expected_signer, parent_chain_id, tee_verifier_address)?;

    let seq_header = sequencer_msg.slice(0..SEQUENCER_HEADER_LEN);
    let res = [
        seq_header.as_ref(),
        cas_cert.downstream_certificate.as_ref(),
    ]
    .concat();
    Ok(res.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::da_api::nitro::certificate::CasCertificate;
    use crate::key_manager::test_utils::test_key_manager;

    #[test]
    fn test_remove_espresso_metadata() {
        let km = test_key_manager();
        let downstream = vec![9u8; 20];
        let espresso_cert =
            CasCertificate::build_espresso_certificate(&km, 0, 0, 0, 0, 0, &downstream).unwrap();

        let mut data = vec![0u8; SEQUENCER_HEADER_LEN];
        let mut expected_data = data.clone();
        data.extend_from_slice(&espresso_cert.to_bytes().unwrap());
        expected_data.extend_from_slice(&downstream);

        let sequencer_msg = Bytes::from(data);

        let extracted = try_extract_da_sequencer_msg_from_espresso_da_cert(
            &sequencer_msg,
            km.signer().address(),
            km.parent_chain_id(),
            km.tee_verifier_address(),
        )
        .unwrap();

        assert_eq!(extracted.len(), SEQUENCER_HEADER_LEN + 20);
        assert_eq!(extracted, Bytes::from(expected_data));
    }

    #[test]
    fn test_extract_espresso_metadata_from_da_certificate() {
        let km = test_key_manager();
        let da_cert = vec![0xAB; 99];
        let espresso_cert =
            CasCertificate::build_espresso_certificate(&km, 0, 0, 0, 0, 0, &da_cert).unwrap();

        let mut data = vec![0u8; SEQUENCER_HEADER_LEN];
        data.extend_from_slice(&espresso_cert.to_bytes().unwrap());
        let sequencer_msg = Bytes::from(data);

        let extracted = try_extract_da_sequencer_msg_from_espresso_da_cert(
            &sequencer_msg,
            km.signer().address(),
            km.parent_chain_id(),
            km.tee_verifier_address(),
        )
        .unwrap();

        assert_eq!(extracted.len(), SEQUENCER_HEADER_LEN + 99);
    }
}
