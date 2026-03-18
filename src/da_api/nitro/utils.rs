use alloy::primitives::Bytes;

use crate::da_api::{
    certificate::nitro::{CASCertificateVersion, CERT_HEADER_SIZE_V1, CasCertificate},
    error::DaApiError,
};

const SEQUENCER_HEADER_LEN: usize = 40;
// const CERT_START: usize = SEQUENCER_HEADER_LEN + DA_CERT_FLAG_LEN;
// const ESPRESSO_CERT_LEN: usize = 149; // 0..148 inclusive

/// Sequencer message format is: [SequencerHeader(40 bytes), Certificate(EspressoCert, Byte1, Byte2, DACert)]
/// this function removes the espresso metadata to obtain the da certificate
/// Returns: [SequencerHeader(40 bytes), DACert]
pub fn extract_da_sequencer_msg_from_espresso_da_certificate(
    sequencer_msg: &Bytes,
) -> Result<Bytes, DaApiError> {
    if sequencer_msg.len() <= SEQUENCER_HEADER_LEN {
        return Err(DaApiError::InvalidSequencerMessageLength(
            SEQUENCER_HEADER_LEN,
            sequencer_msg.len(),
        ));
    }

    let cas_version = CASCertificateVersion::try_from(sequencer_msg[SEQUENCER_HEADER_LEN])?;
    let header_size = match cas_version {
        CASCertificateVersion::V1 => CERT_HEADER_SIZE_V1,
    };

    let seq_msg = sequencer_msg.slice(0..SEQUENCER_HEADER_LEN);

    let da_cert_position = CasCertificate::da_header_start_position(header_size);

    let _espresso_da_cert_format =
        sequencer_msg.slice(SEQUENCER_HEADER_LEN..SEQUENCER_HEADER_LEN + da_cert_position);
    let da_cert = sequencer_msg.slice(40 + da_cert_position + 2..);

    let res = [seq_msg, da_cert].concat();
    Ok(res.into())
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
        // data.push(0x63);

        // espresso certificate metadata (0..145)
        let espresso_cert_length = CasCertificate::da_header_start_position(32);
        let mut cert_metadata: Vec<u8> = (0..espresso_cert_length as u8).collect();
        cert_metadata[0] = 1;
        data.extend(&cert_metadata);

        data.push(0x01);
        data.push(0x63);
        data.extend(vec![9u8; 20]); // dummy downstream cert

        expected_data.extend(vec![9u8; 20]);

        let sequencer_msg = Bytes::from(data);

        let extracted =
            extract_da_sequencer_msg_from_espresso_da_certificate(&sequencer_msg).unwrap();

        // check length
        assert_eq!(extracted.len(), (SEQUENCER_HEADER_LEN + 20));

        // check contents match expected metadata
        assert_eq!(extracted, Bytes::from(expected_data));
    }

    #[test]
    fn test_extract_espresso_metadata_from_da_certificate() {
        //  40+145+1+1+99=286
        // 286-145-1-1=139
        let sequencer_message=Bytes::from_str("0x000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000d6f4495acb1e8e0c5583a2357178fffd13f0cec5b216542b40027999633d72f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001ff01ffa2f5868a6c1f36e948ade0eaf093983af330a1ec8183a61955e4fd8d67313fbd1bc5981b980a01a85bb7c5299545170e1126a6a84b1c9e83719562fbe022d24ae126266b22c4717b69f9b4771a8b0c1d28681ddd0582a55b9fd76286be70cf54dc").unwrap();

        let extracted =
            extract_da_sequencer_msg_from_espresso_da_certificate(&sequencer_message).unwrap();

        assert_eq!(extracted.len(), 139);
    }
}
