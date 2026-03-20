use alloy::primitives::Bytes;

// TODO: Remove once actual verify function is implemented
// mock function
pub fn verify_batch_data(message: Bytes) -> (u32, u32, u32, u32, Vec<u8>) {
    (0, 0, 0, 0, message.to_vec())
}
