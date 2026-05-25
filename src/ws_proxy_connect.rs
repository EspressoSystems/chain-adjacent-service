// WS connector for alloy that respects HTTPS_PROXY.
//
// Inside the Nitro enclave, enclaver exposes an HTTP CONNECT egress proxy
// and sets HTTPS_PROXY. reqwest auto-detects that and proxies HTTPS through
// it transparently. But alloy's default WS transport (tokio-tungstenite via
// alloy-transport-ws) ignores HTTPS_PROXY — it always resolves DNS in the
// enclave and opens a direct TCP socket, which fails because enclave-side
// DNS is not wired up for arbitrary hosts.
//
// This module implements a custom alloy_pubsub::PubSubConnect that:
//   1. Reads HTTPS_PROXY (with lowercase / HTTP_ fallbacks) from env.
//   2. If set: TCP → proxy → CONNECT host:port, then hand the tunnelled
//      stream to tokio-tungstenite which finishes the TLS + WS handshake.
//   3. If unset: TCP → host:port → TLS + WS handshake (local / e2e path).
// The resulting WebSocketStream is bridged to alloy's PubSub channels via a
// backend task that mirrors alloy-transport-ws's WsBackend.

use std::env;
use std::sync::{Arc, OnceLock};

use alloy::pubsub::{ConnectionHandle, ConnectionInterface, PubSubConnect};
use alloy::rpc::json_rpc::PubSubItem;
use alloy::transports::{TransportErrorKind, TransportResult};
use anyhow::{Context, Result, bail};
use async_http_proxy::http_connect_tokio;
use futures::{SinkExt, StreamExt};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;
use yawc::{HttpRequestBuilder, MaybeTlsStream, Options, TcpWebSocket, WebSocket};

#[derive(Debug, Clone)]
pub struct WsProxyConnect {
    url: String,
}

impl WsProxyConnect {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl PubSubConnect for WsProxyConnect {
    fn is_local(&self) -> bool {
        alloy::transports::utils::guess_local_url(&self.url)
    }

    async fn connect(&self) -> TransportResult<ConnectionHandle> {
        let url: Url = self
            .url
            .parse()
            .map_err(|e: url::ParseError| TransportErrorKind::custom_str(&e.to_string()))?;
        let host = url
            .host_str()
            .ok_or_else(|| TransportErrorKind::custom_str("ws url missing host"))?
            .to_string();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| TransportErrorKind::custom_str("ws url missing port"))?;
        match url.scheme() {
            "ws" | "wss" => {}
            other => {
                return Err(TransportErrorKind::custom_str(&format!(
                    "unsupported scheme {other}"
                )));
            }
        }

        let tcp = open_tcp(&host, port, url.scheme() == "wss").await?;

        let request = self
            .url
            .as_str()
            .into_client_request()
            .map_err(TransportErrorKind::custom)?;

        // client_async_tls_with_config handles TLS upgrade (when scheme is wss)
        // and the WS handshake on the already-connected stream.
        let (ws, _) = tokio_tungstenite::client_async_tls_with_config(request, tcp, None, None)
            .await
            .map_err(TransportErrorKind::custom)?;

        let (handle, interface) = ConnectionHandle::new();
        tokio::spawn(run_backend(ws, interface));
        Ok(handle)
    }
}

/// Connect a yawc WebSocket through the same `HTTPS_PROXY` egress path
/// `WsProxyConnect` uses. Equivalent to `WebSocket::connect(url)` but routed
/// through the enclaver HTTP CONNECT proxy when the env var is set, so DNS
/// is resolved by the proxy host rather than the enclave.
///
/// For `wss://` we perform the TLS handshake with `tokio-rustls` against
/// the webpki roots, then hand the resulting stream to
/// `yawc::WebSocket::handshake_with_request` — preserving yawc's
/// permessage-deflate compression and custom upgrade headers.
pub async fn connect_yawc(
    url: Url,
    request: HttpRequestBuilder,
    options: Options,
) -> Result<TcpWebSocket> {
    let host = url.host_str().context("ws url missing host")?.to_string();
    let port = url.port_or_known_default().context("ws url missing port")?;

    let is_secure = url.scheme() == "wss";
    let tcp = open_tcp(&host, port, is_secure)
        .await
        .map_err(|e| anyhow::anyhow!("open_tcp failed: {e}"))?;

    let stream: MaybeTlsStream<TcpStream> = match url.scheme() {
        "ws" => MaybeTlsStream::Plain(tcp),
        "wss" => {
            let connector = tls_connector();
            let server_name = ServerName::try_from(host.clone())
                .with_context(|| format!("invalid DNS name for SNI: {host}"))?;
            let tls = connector
                .connect(server_name, tcp)
                .await
                .with_context(|| format!("TLS handshake to {host}:{port} failed"))?;
            MaybeTlsStream::Tls(tls)
        }
        other => bail!("unsupported scheme {other}"),
    };

    WebSocket::handshake_with_request(url, stream, options, request)
        .await
        .context("yawc handshake failed")
}

fn tls_connector() -> &'static TlsConnector {
    static CONNECTOR: OnceLock<TlsConnector> = OnceLock::new();
    CONNECTOR.get_or_init(|| {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        TlsConnector::from(Arc::new(config))
    })
}

fn select_proxy<F: Fn(&str) -> Option<String>>(is_secure: bool, get: F) -> Option<String> {
    let vars: &[&str] = if is_secure {
        &["HTTPS_PROXY", "https_proxy"]
    } else {
        &["HTTP_PROXY", "http_proxy"]
    };
    // Treat empty values as unset so the next candidate is tried — matches the
    // shell convention of `HTTPS_PROXY=` unsetting the proxy.
    vars.iter().find_map(|k| get(k).filter(|s| !s.is_empty()))
}

async fn open_tcp(
    target_host: &str,
    target_port: u16,
    is_secure: bool,
) -> TransportResult<TcpStream> {
    let proxy = select_proxy(is_secure, |k| env::var(k).ok());

    let Some(proxy_url) = proxy else {
        return TcpStream::connect((target_host, target_port))
            .await
            .map_err(TransportErrorKind::custom);
    };

    let proxy: Url = proxy_url
        .parse()
        .map_err(|e: url::ParseError| TransportErrorKind::custom_str(&e.to_string()))?;
    let proxy_host = proxy
        .host_str()
        .ok_or_else(|| TransportErrorKind::custom_str("proxy url missing host"))?
        .to_string();
    let proxy_port = proxy
        .port_or_known_default()
        .ok_or_else(|| TransportErrorKind::custom_str("proxy url missing port"))?;

    let mut tcp = TcpStream::connect((proxy_host.as_str(), proxy_port))
        .await
        .map_err(TransportErrorKind::custom)?;
    http_connect_tokio(&mut tcp, target_host, target_port)
        .await
        .map_err(TransportErrorKind::custom)?;
    Ok(tcp)
}

type WsStream<S> = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<S>>;

async fn run_backend<S>(mut ws: WsStream<S>, mut interface: ConnectionInterface)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    loop {
        tokio::select! {
            biased;
            outbound = interface.recv_from_frontend() => {
                let Some(req) = outbound else { break };
                if let Err(err) = ws.send(Message::Text(req.get().to_owned().into())).await {
                    tracing::error!(%err, "ws send failed");
                    interface.close_with_error();
                    return;
                }
            }
            inbound = ws.next() => {
                let Some(msg) = inbound else {
                    tracing::debug!("ws stream ended");
                    interface.close_with_error();
                    return;
                };
                let msg = match msg {
                    Ok(m) => m,
                    Err(err) => {
                        tracing::error!(%err, "ws receive failed");
                        interface.close_with_error();
                        return;
                    }
                };
                match msg {
                    Message::Text(text) => {
                        match serde_json::from_str::<PubSubItem>(&text) {
                            Ok(item) => {
                                if interface.send_to_frontend(item).is_err() {
                                    return;
                                }
                            }
                            Err(err) => {
                                tracing::error!(%err, "ws text deserialize failed");
                                interface.close_with_error();
                                return;
                            }
                        }
                    }
                    Message::Close(frame) => {
                        tracing::info!(?frame, "ws server sent close");
                        interface.close_with_error();
                        return;
                    }
                    Message::Ping(payload) => {
                        if let Err(err) = ws.send(Message::Pong(payload)).await {
                            tracing::error!(%err, "ws pong failed");
                            interface.close_with_error();
                            return;
                        }
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Binary(_) => {
                        tracing::error!("unexpected ws binary frame");
                        interface.close_with_error();
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::select_proxy;
    use std::collections::HashMap;

    fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn secure_prefers_https_proxy_uppercase() {
        let env = env_from(&[
            ("HTTPS_PROXY", "http://upper:1"),
            ("https_proxy", "http://lower:2"),
        ]);
        assert_eq!(select_proxy(true, env).as_deref(), Some("http://upper:1"));
    }

    #[test]
    fn secure_falls_back_to_lowercase_https_proxy() {
        let env = env_from(&[("https_proxy", "http://lower:2")]);
        assert_eq!(select_proxy(true, env).as_deref(), Some("http://lower:2"));
    }

    #[test]
    fn secure_ignores_http_proxy() {
        let env = env_from(&[
            ("HTTP_PROXY", "http://upper:1"),
            ("http_proxy", "http://lower:2"),
        ]);
        assert_eq!(select_proxy(true, env), None);
    }

    #[test]
    fn insecure_prefers_http_proxy_uppercase() {
        let env = env_from(&[
            ("HTTP_PROXY", "http://upper:1"),
            ("http_proxy", "http://lower:2"),
        ]);
        assert_eq!(select_proxy(false, env).as_deref(), Some("http://upper:1"));
    }

    #[test]
    fn insecure_ignores_https_proxy() {
        let env = env_from(&[
            ("HTTPS_PROXY", "http://upper:1"),
            ("https_proxy", "http://lower:2"),
        ]);
        assert_eq!(select_proxy(false, env), None);
    }

    #[test]
    fn empty_string_is_treated_as_unset() {
        let env = env_from(&[("HTTPS_PROXY", ""), ("https_proxy", "http://lower:2")]);
        assert_eq!(select_proxy(true, env).as_deref(), Some("http://lower:2"));
    }

    #[test]
    fn all_empty_returns_none() {
        let env = env_from(&[("HTTPS_PROXY", ""), ("https_proxy", "")]);
        assert_eq!(select_proxy(true, env), None);
    }

    #[test]
    fn unset_returns_none() {
        assert_eq!(select_proxy(true, |_| None), None);
        assert_eq!(select_proxy(false, |_| None), None);
    }
}
