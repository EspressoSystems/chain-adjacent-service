use alloy::primitives::Bytes;

use crate::da_api::{error::DaApiError, nitro::certificate::CasCertificate};

pub const SEQUENCER_HEADER_LEN: usize = 40;

/// Sequencer message format is: [SequencerHeader(40 bytes), EspressoCert(101 bytes), DACert]
/// this function parses and validates the CAS certificate that follows the sequencer header,
/// then strips the espresso metadata to obtain the downstream da certificate.
/// Returns: [SequencerHeader(40 bytes), DACert]
pub fn try_extract_da_sequencer_msg_from_espresso_da_cert(
    sequencer_msg: &Bytes,
) -> Result<Bytes, DaApiError> {
    if sequencer_msg.len() <= SEQUENCER_HEADER_LEN {
        return Err(DaApiError::InvalidSequencerMessageLength(
            SEQUENCER_HEADER_LEN,
            sequencer_msg.len(),
        ));
    }

    let cas_cert = CasCertificate::from_bytes(&sequencer_msg[SEQUENCER_HEADER_LEN..])?;
    cas_cert.validate()?;

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
    use std::str::FromStr;

    use super::*;
    use crate::da_api::nitro::certificate::CASCertificateVersion;

    #[test]
    fn test_remove_espresso_metadata() {
        // build fake sequencer header
        let mut data = vec![0u8; SEQUENCER_HEADER_LEN];
        let mut expected_data = data.clone();

        // espresso certificate metadata (101 bytes), with the cas version byte at the start
        let espresso_cert_length = CasCertificate::da_header_start_position(32);
        let mut cert_metadata: Vec<u8> = (0..espresso_cert_length as u8).collect();
        cert_metadata[0] = CASCertificateVersion::V0 as u8;
        data.extend(&cert_metadata);

        data.extend(vec![9u8; 20]); // dummy downstream cert

        expected_data.extend(vec![9u8; 20]);

        let sequencer_msg = Bytes::from(data);

        let extracted = try_extract_da_sequencer_msg_from_espresso_da_cert(&sequencer_msg).unwrap();

        // check length
        assert_eq!(extracted.len(), (SEQUENCER_HEADER_LEN + 20));

        // check contents match expected metadata
        assert_eq!(extracted, Bytes::from(expected_data));
    }

    #[test]
    fn test_extract_espresso_metadata_from_da_certificate() {
        // 40 (seq header) + 101 (espresso cert metadata, version 0x70 at byte 40) + 99 (da cert) = 240
        // extracted = 40 + 99 = 139
        let sequencer_message=Bytes::from_str("0x00000000000000000000000000000000000000000000000000000000000000000000000000000000700000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1bc93d7c92fd65dbd4809a2dcfd0f31c201f52aedbb700e3462d6cc1058ec2ac194723c2f1d41d7a65c1d2cf9a0683fe6a458ac269aaeb00c1b0cf8854afc05166").unwrap();

        let extracted =
            try_extract_da_sequencer_msg_from_espresso_da_cert(&sequencer_message).unwrap();

        assert_eq!(extracted.len(), 139);
    }
}
