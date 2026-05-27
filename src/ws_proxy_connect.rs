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
// The resulting WebSocketStream is bridged to alloy's PubSub channels by
// handing it to `alloy_transport_ws::WsBackend::from_socket(...).spawn()`,
// which provides keepalive ping/pong and the standard message dispatch.

use std::env;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use alloy::pubsub::{ConnectionHandle, PubSubConnect};
use alloy::transports::ws::WsBackend;
use alloy::transports::{TransportErrorKind, TransportResult};
use anyhow::{Context, Result, bail};
use async_http_proxy::http_connect_tokio;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;
use yawc::{HttpRequestBuilder, MaybeTlsStream, Options, TcpWebSocket, WebSocket};

/// Keepalive ping interval for the alloy WebSocket backend. Matches
/// `alloy-transport-ws`'s own `DEFAULT_KEEPALIVE`.
const WS_KEEPALIVE: Duration = Duration::from_secs(10);

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
        WsBackend::from_socket(ws, interface, WS_KEEPALIVE).spawn();
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
