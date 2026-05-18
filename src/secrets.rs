use std::env;

use anyhow::{Context, Result, bail};
use aws_config::{BehaviorVersion, Region};
use serde::Deserialize;

use crate::config::ServiceConfig;
use crate::rollups::nitro::config::NitroConfig;

pub const PLACEHOLDER_STR: &str = "PLACEHOLDER";

pub const ENV_AWS_REGION: &str = "AWS_REGION";
pub const ENV_AWS_SECRET_ID: &str = "AWS_SECRET_ID";

#[derive(Debug, Clone, Deserialize)]
pub struct SecretOverrides {
    pub espresso_base_url: String,
    pub feed_ws_url: String,
    pub l1_ws_url: String,
    pub anytrust_endpoint: String,
}

pub async fn fetch_secret_overrides() -> Result<SecretOverrides> {
    let region = env::var(ENV_AWS_REGION)
        .with_context(|| format!("{ENV_AWS_REGION} env var must be set"))?;
    let secret_id = env::var(ENV_AWS_SECRET_ID)
        .with_context(|| format!("{ENV_AWS_SECRET_ID} env var must be set"))?;

    let aws_cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region))
        .load()
        .await;
    let client = aws_sdk_secretsmanager::Client::new(&aws_cfg);

    let resp = client
        .get_secret_value()
        .secret_id(&secret_id)
        .send()
        .await
        .with_context(|| format!("fetching secret {secret_id}"))?;

    let secret_str = resp
        .secret_string()
        .context("secret has no SecretString payload")?;

    parse_secret_overrides(secret_str)
}

pub fn parse_secret_overrides(secret_str: &str) -> Result<SecretOverrides> {
    // Accept two shapes:
    //   (a) direct  — the secret IS the SecretOverrides JSON.
    //   (b) wrapped — the secret is `{"parameters": "<stringified-overrides-json>"}`.
    //                 This is how our infra/tooling stores it in AWS today.
    if let Ok(overrides) = serde_json::from_str::<SecretOverrides>(secret_str) {
        return Ok(overrides);
    }
    #[derive(Deserialize)]
    struct Wrapper {
        parameters: String,
    }
    let wrapper: Wrapper = serde_json::from_str(secret_str)
        .context("parsing secret JSON (neither direct nor `{\"parameters\": \"...\"}` shape)")?;
    serde_json::from_str(&wrapper.parameters).context("parsing inner `parameters` JSON")
}

pub fn apply_overrides_nitro(
    cfg: &mut ServiceConfig<NitroConfig>,
    overrides: &SecretOverrides,
) -> Result<()> {
    cfg.espresso_client.base_url = url::Url::parse(&overrides.espresso_base_url)
        .context("overrides.espresso_base_url is not a valid URL")?;
    tracing::info!(
        field = "espresso_client.base_url",
        "applied secret override"
    );

    cfg.rollup.stack.feed.web_socket_url = overrides.feed_ws_url.clone();
    tracing::info!(
        field = "rollup.stack.feed.web_socket_url",
        "applied secret override"
    );

    cfg.rollup.stack.l1_ws_url = overrides.l1_ws_url.clone();
    tracing::info!(field = "rollup.stack.l1_ws_url", "applied secret override");

    let mut anytrust_applied = 0usize;
    for provider in cfg.da_server.da_providers.iter_mut() {
        if provider.is_anytrust {
            provider.endpoint_url = overrides.anytrust_endpoint.clone();
            anytrust_applied += 1;
            tracing::info!(
                field = "da_server.da_providers[is_anytrust].endpoint_url",
                provider_name = %provider.name,
                "applied secret override"
            );
        }
    }
    if anytrust_applied == 0 {
        tracing::info!(
            "no anytrust provider configured; anytrust_endpoint secret value not applied"
        );
    }

    Ok(())
}

pub fn assert_no_placeholders_nitro(cfg: &ServiceConfig<NitroConfig>) -> Result<()> {
    if cfg.espresso_client.base_url.as_str() == PLACEHOLDER_STR {
        bail!("espresso_client.base_url was not overridden — still placeholder sentinel");
    }
    if cfg.rollup.stack.feed.web_socket_url == PLACEHOLDER_STR {
        bail!("rollup.stack.feed.web_socket_url was not overridden — still PLACEHOLDER");
    }
    if cfg.rollup.stack.l1_ws_url == PLACEHOLDER_STR {
        bail!("rollup.stack.l1_ws_url was not overridden — still PLACEHOLDER");
    }
    for provider in &cfg.da_server.da_providers {
        if provider.is_anytrust && provider.endpoint_url == PLACEHOLDER_STR {
            bail!(
                "da_provider {} endpoint_url not overridden — still PLACEHOLDER",
                provider.name
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placeholder_config_json() -> &'static str {
        r#"{
            "espresso_client": {
                "base_url": "http://placeholder.invalid/"
            },
            "rollup": {
                "type": "nitro",
                "namespace_id": 1,
                "stack": {
                    "chain_id": 412346,
                    "feed": {
                        "web_socket_url": "PLACEHOLDER",
                        "current_message_count": 0
                    },
                    "l1_ws_url": "PLACEHOLDER",
                    "sequencer_inbox_address": "0x0000000000000000000000000000000000000000"
                }
            },
            "da_server": {
                "listen_addr": "0.0.0.0:9000",
                "da_providers": [
                    {
                        "name": "anytrust",
                        "endpoint_url": "PLACEHOLDER",
                        "is_anytrust": true
                    },
                    {
                        "name": "calldata",
                        "endpoint_url": "",
                        "is_anytrust": false
                    }
                ]
            },
            "is_fresh_deployment": true
        }"#
    }

    fn sample_secret_json() -> &'static str {
        r#"{
            "espresso_base_url": "https://query.example.com/",
            "feed_ws_url": "wss://feed.example.com/feed",
            "l1_ws_url": "wss://l1.example.com",
            "anytrust_endpoint": "http://anytrust.example.com:9876"
        }"#
    }

    #[test]
    fn parses_secret_json() {
        let overrides = parse_secret_overrides(sample_secret_json()).unwrap();
        assert_eq!(overrides.espresso_base_url, "https://query.example.com/");
        assert_eq!(overrides.feed_ws_url, "wss://feed.example.com/feed");
        assert_eq!(overrides.l1_ws_url, "wss://l1.example.com");
        assert_eq!(
            overrides.anytrust_endpoint,
            "http://anytrust.example.com:9876"
        );
    }

    #[test]
    fn parses_wrapped_secret_json() {
        let wrapped = r#"{"parameters":"{ \"espresso_base_url\": \"https://query.example.com/\", \"feed_ws_url\": \"wss://feed.example.com/feed\", \"l1_ws_url\": \"wss://l1.example.com\", \"anytrust_endpoint\": \"http://anytrust.example.com:9876\" }"}"#;
        let overrides = parse_secret_overrides(wrapped).unwrap();
        assert_eq!(overrides.espresso_base_url, "https://query.example.com/");
        assert_eq!(overrides.feed_ws_url, "wss://feed.example.com/feed");
        assert_eq!(overrides.l1_ws_url, "wss://l1.example.com");
        assert_eq!(
            overrides.anytrust_endpoint,
            "http://anytrust.example.com:9876"
        );
    }

    #[test]
    fn parsing_fails_when_field_missing() {
        let bad = r#"{
            "espresso_base_url": "https://query.example.com/",
            "feed_ws_url": "wss://feed.example.com/feed",
            "l1_ws_url": "wss://l1.example.com"
        }"#;
        assert!(parse_secret_overrides(bad).is_err());
    }

    #[test]
    fn apply_and_assert_replaces_all_placeholders() {
        let mut cfg: ServiceConfig<NitroConfig> =
            serde_json::from_str(placeholder_config_json()).unwrap();

        assert!(assert_no_placeholders_nitro(&cfg).is_err());

        let overrides = parse_secret_overrides(sample_secret_json()).unwrap();
        apply_overrides_nitro(&mut cfg, &overrides).unwrap();

        assert_eq!(
            cfg.espresso_client.base_url.as_str(),
            "https://query.example.com/"
        );
        assert_eq!(
            cfg.rollup.stack.feed.web_socket_url,
            "wss://feed.example.com/feed"
        );
        assert_eq!(cfg.rollup.stack.l1_ws_url, "wss://l1.example.com");

        let anytrust = cfg
            .da_server
            .da_providers
            .iter()
            .find(|p| p.is_anytrust)
            .unwrap();
        assert_eq!(anytrust.endpoint_url, "http://anytrust.example.com:9876");

        let calldata = cfg
            .da_server
            .da_providers
            .iter()
            .find(|p| p.name == "calldata")
            .unwrap();
        assert_eq!(calldata.endpoint_url, "");

        assert_no_placeholders_nitro(&cfg).unwrap();
    }

    #[test]
    fn assert_fails_when_l1_ws_url_left_as_placeholder() {
        let mut cfg: ServiceConfig<NitroConfig> =
            serde_json::from_str(placeholder_config_json()).unwrap();
        let mut overrides = parse_secret_overrides(sample_secret_json()).unwrap();

        overrides.l1_ws_url = PLACEHOLDER_STR.to_string();
        apply_overrides_nitro(&mut cfg, &overrides).unwrap();

        let err = assert_no_placeholders_nitro(&cfg).unwrap_err();
        assert!(err.to_string().contains("l1_ws_url"));
    }

    #[test]
    fn assert_fails_when_base_url_left_as_sentinel() {
        let cfg: ServiceConfig<NitroConfig> =
            serde_json::from_str(placeholder_config_json()).unwrap();
        // No overrides applied — base_url is still the sentinel.
        let err = assert_no_placeholders_nitro(&cfg).unwrap_err();
        assert!(err.to_string().contains("espresso_client.base_url"));
    }

    // Live integration test against AWS Secrets Manager.
    //
    // Marked `#[ignore]` so it doesn't run by default. To execute:
    //   1. cp .env.example .env  (then fill in AWS_REGION / AWS_SECRET_ID and
    //      any credential vars you need)
    //   2. cargo test --lib secrets::tests::live_fetch -- --ignored --nocapture
    //   3. may need to run with `AWS_PROFILE=nitro-devnets` depending on your credential setup
    //
    #[tokio::test]
    #[ignore]
    async fn live_fetch_against_aws_secrets_manager() {
        let _ = dotenvy::dotenv();

        let overrides = fetch_secret_overrides()
            .await
            .expect("fetch_secret_overrides failed — check AWS_REGION, AWS_SECRET_ID, credentials, and secret JSON shape");

        assert!(
            !overrides.espresso_base_url.is_empty(),
            "espresso_base_url is empty"
        );
        assert!(!overrides.feed_ws_url.is_empty(), "feed_ws_url is empty");
        assert!(!overrides.l1_ws_url.is_empty(), "l1_ws_url is empty");
        assert!(
            !overrides.anytrust_endpoint.is_empty(),
            "anytrust_endpoint is empty"
        );

        url::Url::parse(&overrides.espresso_base_url)
            .expect("espresso_base_url is not a valid URL");

        println!(
            "fetched secret with fields: espresso_base_url, feed_ws_url, l1_ws_url, anytrust_endpoint"
        );
    }
}
