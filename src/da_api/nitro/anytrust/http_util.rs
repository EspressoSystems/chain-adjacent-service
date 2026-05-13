//! Shared HTTP helpers for the AnyTrust modules.

use crate::da_api::error::DaApiError;

/// Max bytes we'll accept in a `das_store` JSON-RPC response. Real responses
/// are ~300 B of JSON (a fixed-size StoreResult), so 64 KB gives ample slack
/// for proxies/wrapping while bounding memory if a backend misbehaves.
pub const DAS_STORE_RESPONSE_LIMIT: usize = 64 * 1024;

/// Max bytes we'll accept in a `get-by-hash` REST response. The body wraps a
/// base64-encoded preimage, so the limit must cover a full batch with ~33%
/// base64 overhead. 32 MB comfortably fits any plausible AnyTrust batch.
pub const GET_BY_HASH_RESPONSE_LIMIT: usize = 32 * 1024 * 1024;

/// Read a response body into memory, refusing once `max_bytes` is exceeded.
/// A pre-check on `Content-Length` lets us reject obviously-too-large bodies
/// before allocating, but a missing/lying header still gets caught while we
/// stream chunks.
pub async fn read_body_bounded(
    mut resp: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, DaApiError> {
    if let Some(declared) = resp.content_length()
        && declared as usize > max_bytes
    {
        return Err(DaApiError::DownstreamDa(format!(
            "response Content-Length {declared} exceeds limit {max_bytes}"
        )));
    }

    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| DaApiError::ParsingError(e.to_string()))?
    {
        if buf.len().saturating_add(chunk.len()) > max_bytes {
            return Err(DaApiError::DownstreamDa(format!(
                "response exceeded {max_bytes} bytes"
            )));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};

    async fn spawn(payload: Vec<u8>) -> String {
        let app = Router::new().route(
            "/",
            get(move || {
                let p = payload.clone();
                async move { (StatusCode::OK, p).into_response() }
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
        format!("http://{bound}")
    }

    fn no_proxy() -> reqwest::Client {
        reqwest::Client::builder().no_proxy().build().unwrap()
    }

    #[tokio::test]
    async fn read_body_bounded_accepts_small_body() {
        let url = spawn(b"hello".to_vec()).await;
        let resp = no_proxy().get(&url).send().await.unwrap();
        let got = read_body_bounded(resp, 1024).await.unwrap();
        assert_eq!(got, b"hello");
    }

    #[tokio::test]
    async fn read_body_bounded_rejects_oversized_body_via_content_length() {
        let url = spawn(vec![0u8; 2048]).await;
        let resp = no_proxy().get(&url).send().await.unwrap();
        let err = read_body_bounded(resp, 100).await.unwrap_err();
        assert!(matches!(err, DaApiError::DownstreamDa(_)), "got: {err}");
    }
}
