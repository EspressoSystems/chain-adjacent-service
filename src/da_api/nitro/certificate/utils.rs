use crate::da_api::error::{DaApiError, DaApiResult};

pub(crate) struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

pub(crate) struct Decoder<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.position
    }

    pub fn read_bytes(&mut self, len: usize) -> DaApiResult<&'a [u8]> {
        if self.remaining() < len {
            return Err(DaApiError::InvalidCertificateLength(self.data.len()));
        }
        let slice = &self.data[self.position..self.position + len];
        self.position += len;
        Ok(slice)
    }

    pub fn read_u32(&mut self) -> DaApiResult<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().map_err(|err| {
            DaApiError::DecoderError(format!("Invalid u32:{err}"))
        })?))
    }

    pub fn read_fixed<const N: usize>(&mut self) -> DaApiResult<[u8; N]> {
        let bytes = self.read_bytes(N)?;
        bytes
            .try_into()
            .map_err(|err| DaApiError::DecoderError(format!("Invalid fixed:{err}")))
    }

    pub fn read_rest(&mut self) -> &'a [u8] {
        let slice = &self.data[self.position..];
        self.position = self.data.len();
        slice
    }
}
