use alloy::primitives::Bytes;

use crate::da_api::error::DaApiError;

const SEQUENCER_HEADER_LEN: usize = 40;
const DA_CERT_FLAG_LEN: usize = 1;
const CERT_START: usize = SEQUENCER_HEADER_LEN + DA_CERT_FLAG_LEN;
const ESPRESSO_CERT_LEN: usize = 149; // 0..148 inclusive

/// Sequencer message format is: [SequencerHeader(40 bytes), DACertificateFlag(0x01), Certificate(EspressoCert, DACert)]
/// this function removes the espresso metadata to obtain the da certificate
/// Returns: [SequencerHeader(40 bytes), DACertificateFlag(0x01), DACert]
pub fn extract_da_sequencer_msg_from_espresso_da_certificate(
    sequencer_msg: &Bytes,
) -> Result<Bytes, DaApiError> {
    if sequencer_msg.len() < CERT_START + ESPRESSO_CERT_LEN {
        return Err(DaApiError::InvalidSequencerMessageLength(
            CERT_START + ESPRESSO_CERT_LEN,
            sequencer_msg.len(),
        ));
    }
    let seq_msg = sequencer_msg.slice(0..SEQUENCER_HEADER_LEN);
    let header_byte = sequencer_msg.slice(SEQUENCER_HEADER_LEN..SEQUENCER_HEADER_LEN + 1);
    let _espresso_da_cert_format = sequencer_msg.slice(CERT_START..CERT_START + ESPRESSO_CERT_LEN);
    let da_cert = sequencer_msg.slice(CERT_START + ESPRESSO_CERT_LEN..);

    let res = [seq_msg, header_byte, da_cert].concat();
    Ok(res.into())
}

/// Input: Certificate format: [SequencerHeader(40 bytes), DACertificateFlag(0x01), Certificate(EspressoCert, DACert)]
/// Returns: EspressoCert
pub fn get_espresso_metadata_from_da_certificate(certificate: &Bytes) -> anyhow::Result<Bytes> {
    if certificate.len() < ESPRESSO_CERT_LEN {
        return Err(anyhow::anyhow!("DA certificate is too short"));
    }

    Ok(certificate.slice(0..ESPRESSO_CERT_LEN))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_remove_espresso_metadata() {
        // build fake sequencer header
        let mut data = vec![0u8; SEQUENCER_HEADER_LEN];
        let mut expected_data = data.clone();

        // DACertificateMessageHeaderFlag
        data.push(0x63);

        // certificate metadata (0..148)
        let cert_metadata: Vec<u8> = (0..ESPRESSO_CERT_LEN as u8).collect();
        data.extend(&cert_metadata);

        // downstream DA fields
        data.push(0x01); // DA API header
        data.push(0x63); // Celestia indicator
        data.extend(vec![9u8; 20]); // dummy downstream cert

        expected_data.push(0x63);
        expected_data.push(0x01);
        expected_data.push(0x63);
        expected_data.extend(vec![9u8; 20]);

        let sequencer_msg = Bytes::from(data);

        let extracted =
            extract_da_sequencer_msg_from_espresso_da_certificate(&sequencer_msg).unwrap();

        // check length
        assert_eq!(extracted.len(), (SEQUENCER_HEADER_LEN + 1 + 1 + 1 + 20));

        // check contents match expected metadata
        assert_eq!(extracted, Bytes::from(expected_data));
    }

    #[test]
    fn test_extract_espresso_metadata_from_da_certificate() {
        let sequencer_message=Bytes::from_str("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000063200100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000d6f4495acb1e8e0c5583a2357178fffd13f0cec5b216542b40027999633d72f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ff01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1c4a3a991487b304c790fd36d080c164f21b819b1ac35393e92940165f3934e130775b12208c995cd6675c5f33c181b19c3657910f4260cc0d115e413d62223db2").unwrap();

        let extracted =
            extract_da_sequencer_msg_from_espresso_da_certificate(&sequencer_message).unwrap();

        assert_eq!(extracted.len(), 139);
    }
}
