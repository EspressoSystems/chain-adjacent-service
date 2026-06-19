use std::env;

use anyhow::{Context, Result, bail};
use aws_config::{BehaviorVersion, Region};
use light_client::state::Genesis;
use serde::Deserialize;

use crate::config::{ServiceConfig, TeeType};
use crate::rollups::nitro::config::NitroConfig;

pub const PLACEHOLDER_STR: &str = "PLACEHOLDER";
/// Sentinel value used for `espresso_client.base_url` in the on-disk config.
/// Distinct from `PLACEHOLDER_STR` because that field is typed as `Url` and
/// must deserialize from a parseable URL.
pub const PLACEHOLDER_URL: &str = "http://placeholder.invalid/";

pub const ENV_AWS_REGION: &str = "AWS_REGION";
pub const ENV_AWS_SECRET_ID: &str = "AWS_SECRET_ID";
pub const ENV_AWS_GENESIS_SECRET_ID: &str = "AWS_GENESIS_SECRET_ID";
pub const ENV_OPERATOR_PRIVATE_KEY: &str = "OPERATOR_PRIVATE_KEY";

#[derive(Debug, Clone, Deserialize)]
pub struct SecretOverrides {
    pub espresso_base_url: String,
    pub feed_ws_url: String,
    pub l1_ws_url: String,
    pub l1_http_url: url::Url,
    pub anytrust_endpoint: String,
    pub starting_hotshot_height: u64,
    pub is_fresh_deployment: bool,
    #[serde(default)]
    pub operator_private_key: Option<String>,
}

/// Fetches the secret overrides from AWS Secrets Manager.
///
/// Returns `Ok(None)` when **both** `AWS_REGION` and `AWS_SECRET_ID` are
/// unset — this lets local/e2e runs ship a fully-baked config file and skip
/// AWS entirely. Returns `Err` if only one of the two is set (operator
/// misconfig) or if the fetch itself fails.
///
/// Production safety is preserved by `assert_no_placeholders_nitro`, which
/// runs unconditionally after the (optional) merge: an operator who forgets
/// the env vars but ships a placeholder-laden config will still fail at
/// startup.
pub async fn fetch_secret_overrides(tee_type: TeeType) -> Result<Option<SecretOverrides>> {
    let region = env::var(ENV_AWS_REGION).ok();
    let secret_id = env::var(ENV_AWS_SECRET_ID).ok();

    let (region, secret_id) = match (region, secret_id) {
        (None, None) => {
            if tee_type == TeeType::Nitro {
                bail!("{ENV_AWS_REGION} and {ENV_AWS_SECRET_ID} not set but Tee type is Nitro");
            } else {
                tracing::info!(
                    "{ENV_AWS_REGION} and {ENV_AWS_SECRET_ID} not set; skipping AWS secret fetch"
                );
                return Ok(None);
            }
        }
        (Some(_), None) | (None, Some(_)) => {
            bail!("{ENV_AWS_REGION} and {ENV_AWS_SECRET_ID} must both be set or both unset");
        }
        (Some(r), Some(s)) => (r, s),
    };

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

    parse_secret_overrides(secret_str).map(Some)
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
    cfg.espresso_client.client.base_url = url::Url::parse(&overrides.espresso_base_url)
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

    cfg.rollup.stack.l1_http_url = overrides.l1_http_url.to_string();
    tracing::info!(
        field = "rollup.stack.l1_http_url",
        "applied secret override"
    );

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

    cfg.streamer.starting_hotshot_height = overrides.starting_hotshot_height;
    tracing::info!(
        field = "streamer.starting_hotshot_height",
        value = overrides.starting_hotshot_height,
        "applied secret override"
    );

    cfg.is_fresh_deployment = overrides.is_fresh_deployment;
    tracing::info!(
        field = "is_fresh_deployment",
        value = overrides.is_fresh_deployment,
        "applied secret override"
    );

    Ok(())
}

/// Fetches the light client genesis from a dedicated secret (`AWS_GENESIS_SECRET_ID`).
/// Returns `Ok(None)` when the env var is unset — local/e2e runs skip this and rely on
/// the genesis baked into the config file. Required in production (Nitro TEE).
pub async fn fetch_genesis(region: &str, tee_type: TeeType) -> Result<Option<Genesis>> {
    let secret_id = match env::var(ENV_AWS_GENESIS_SECRET_ID).ok() {
        Some(id) => id,
        None => {
            if tee_type == TeeType::Nitro {
                bail!("{ENV_AWS_GENESIS_SECRET_ID} not set but Tee type is Nitro");
            }
            tracing::info!("{ENV_AWS_GENESIS_SECRET_ID} not set; skipping genesis secret fetch");
            return Ok(None);
        }
    };

    let aws_cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region.to_string()))
        .load()
        .await;
    let client = aws_sdk_secretsmanager::Client::new(&aws_cfg);

    let resp = client
        .get_secret_value()
        .secret_id(&secret_id)
        .send()
        .await
        .with_context(|| format!("fetching genesis secret {secret_id}"))?;

    let secret_str = resp
        .secret_string()
        .context("genesis secret has no SecretString payload")?;

    #[derive(Deserialize)]
    struct GenesisSecret {
        light_client_genesis: Genesis,
    }
    let parsed: GenesisSecret =
        serde_json::from_str(secret_str).context("parsing genesis secret JSON")?;

    tracing::info!(
        field = "espresso_client.light_client.genesis",
        secret_id,
        "fetched genesis from secret"
    );
    Ok(Some(parsed.light_client_genesis))
}

/// Resolves the operator private key from AWS secret overrides, falling back
/// to the `OPERATOR_PRIVATE_KEY` env var if the secret omits the field (or no
/// AWS overrides were fetched). Errors only when both sources are missing.
pub fn resolve_operator_private_key(overrides: Option<&SecretOverrides>) -> Result<String> {
    if let Some(key) = overrides.and_then(|o| o.operator_private_key.as_ref()) {
        tracing::info!("operator_private_key sourced from AWS secret");
        return Ok(key.clone());
    }
    match env::var(ENV_OPERATOR_PRIVATE_KEY) {
        Ok(key) => {
            tracing::info!("operator_private_key sourced from {ENV_OPERATOR_PRIVATE_KEY} env var");
            Ok(key)
        }
        Err(_) => bail!(
            "operator_private_key not found in AWS secret and {ENV_OPERATOR_PRIVATE_KEY} env var is not set"
        ),
    }
}

pub fn assert_no_placeholders_nitro(cfg: &ServiceConfig<NitroConfig>) -> Result<()> {
    if cfg.espresso_client.client.base_url.as_str() == PLACEHOLDER_URL {
        bail!("espresso_client.base_url was not overridden — still placeholder sentinel");
    }
    if cfg.rollup.stack.feed.web_socket_url == PLACEHOLDER_STR {
        bail!("rollup.stack.feed.web_socket_url was not overridden — still PLACEHOLDER");
    }
    if cfg.rollup.stack.l1_ws_url == PLACEHOLDER_STR {
        bail!("rollup.stack.l1_ws_url was not overridden — still PLACEHOLDER");
    }
    if cfg.rollup.stack.l1_http_url == PLACEHOLDER_STR {
        bail!("rollup.stack.l1_http_url was not overridden — still PLACEHOLDER");
    }
    url::Url::parse(&cfg.rollup.stack.l1_http_url)
        .context("rollup.stack.l1_http_url is not a valid URL")?;
    for provider in &cfg.da_server.da_providers {
        if provider.is_anytrust && provider.endpoint_url == PLACEHOLDER_STR {
            bail!(
                "da_provider {} endpoint_url not overridden — still PLACEHOLDER",
                provider.name
            );
        }
    }
    if cfg.key_manager.tee_type == TeeType::Nitro
        && cfg
            .espresso_client
            .light_client
            .genesis
            .stake_table
            .is_empty()
    {
        bail!(
            "espresso_client.light_client.genesis.stake_table is empty — genesis was not overridden from secret"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placeholder_config_json() -> &'static str {
        r#"{
            "espresso_client": {
                "base_url": "http://placeholder.invalid/",
                "light_client": {
                    "genesis": {
                        "epoch_height": 100,
                        "first_epoch_with_dynamic_stake_table": 1,
                        "stake_table": []
                    }
                }
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
                    "l1_http_url": "PLACEHOLDER",
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
            "key_manager": {
                "tee_verifier_address": "0x0000000000000000000000000000000000000000",
                "attestation_verifier_url": "http://localhost:9000",
                "tee_type": "test"
            },
            "is_fresh_deployment": false
        }"#
    }

    fn sample_secret_json() -> &'static str {
        r#"{
            "espresso_base_url": "https://query.example.com/",
            "feed_ws_url": "wss://feed.example.com/feed",
            "l1_ws_url": "wss://l1.example.com",
            "l1_http_url": "https://l1.example.com",
            "anytrust_endpoint": "http://anytrust.example.com:9876",
            "starting_hotshot_height": 9199257,
            "is_fresh_deployment": true
        }"#
    }

    #[test]
    fn parses_secret_json() {
        let overrides = parse_secret_overrides(sample_secret_json()).unwrap();
        assert_eq!(overrides.espresso_base_url, "https://query.example.com/");
        assert_eq!(overrides.feed_ws_url, "wss://feed.example.com/feed");
        assert_eq!(overrides.l1_ws_url, "wss://l1.example.com");
        assert_eq!(overrides.l1_http_url.as_str(), "https://l1.example.com/");
        assert_eq!(
            overrides.anytrust_endpoint,
            "http://anytrust.example.com:9876"
        );
        assert_eq!(overrides.starting_hotshot_height, 9199257);
        assert!(overrides.is_fresh_deployment);
    }

    #[test]
    fn parses_wrapped_secret_json() {
        let wrapped = r#"{"parameters":"{ \"espresso_base_url\": \"https://query.example.com/\", \"feed_ws_url\": \"wss://feed.example.com/feed\", \"l1_ws_url\": \"wss://l1.example.com\", \"l1_http_url\": \"https://l1.example.com\", \"anytrust_endpoint\": \"http://anytrust.example.com:9876\", \"starting_hotshot_height\": 9199257, \"is_fresh_deployment\": true }"}"#;
        let overrides = parse_secret_overrides(wrapped).unwrap();
        assert_eq!(overrides.espresso_base_url, "https://query.example.com/");
        assert_eq!(overrides.feed_ws_url, "wss://feed.example.com/feed");
        assert_eq!(overrides.l1_ws_url, "wss://l1.example.com");
        assert_eq!(overrides.l1_http_url.as_str(), "https://l1.example.com/");
        assert_eq!(
            overrides.anytrust_endpoint,
            "http://anytrust.example.com:9876"
        );
        assert_eq!(overrides.starting_hotshot_height, 9199257);
        assert!(overrides.is_fresh_deployment);
    }

    #[test]
    fn parsing_fails_when_l1_http_url_malformed() {
        let bad = r#"{
            "espresso_base_url": "https://query.example.com/",
            "feed_ws_url": "wss://feed.example.com/feed",
            "l1_ws_url": "wss://l1.example.com",
            "l1_http_url": "not a url",
            "anytrust_endpoint": "http://anytrust.example.com:9876",
            "starting_hotshot_height": 9199257,
            "is_fresh_deployment": true
        }"#;
        assert!(parse_secret_overrides(bad).is_err());
    }

    #[test]
    fn parsing_fails_when_starting_hotshot_height_missing() {
        let bad = r#"{
            "espresso_base_url": "https://query.example.com/",
            "feed_ws_url": "wss://feed.example.com/feed",
            "l1_ws_url": "wss://l1.example.com",
            "l1_http_url": "https://l1.example.com",
            "anytrust_endpoint": "http://anytrust.example.com:9876",
            "is_fresh_deployment": true
        }"#;
        assert!(parse_secret_overrides(bad).is_err());
    }

    #[test]
    fn parsing_fails_when_is_fresh_deployment_missing() {
        let bad = r#"{
            "espresso_base_url": "https://query.example.com/",
            "feed_ws_url": "wss://feed.example.com/feed",
            "l1_ws_url": "wss://l1.example.com",
            "l1_http_url": "https://l1.example.com",
            "anytrust_endpoint": "http://anytrust.example.com:9876",
            "starting_hotshot_height": 9199257
        }"#;
        assert!(parse_secret_overrides(bad).is_err());
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
    fn operator_private_key_field_is_optional_in_secret() {
        let overrides = parse_secret_overrides(sample_secret_json()).unwrap();
        assert!(overrides.operator_private_key.is_none());
    }

    #[test]
    fn parses_operator_private_key_when_present() {
        let with_key = r#"{
            "espresso_base_url": "https://query.example.com/",
            "feed_ws_url": "wss://feed.example.com/feed",
            "l1_ws_url": "wss://l1.example.com",
            "l1_http_url": "https://l1.example.com",
            "anytrust_endpoint": "http://anytrust.example.com:9876",
            "starting_hotshot_height": 9199257,
            "is_fresh_deployment": true,
            "operator_private_key": "0xabc123"
        }"#;
        let overrides = parse_secret_overrides(with_key).unwrap();
        assert_eq!(overrides.operator_private_key.as_deref(), Some("0xabc123"));
    }

    #[test]
    fn resolve_operator_private_key_prefers_aws_value() {
        let overrides = SecretOverrides {
            espresso_base_url: "https://x/".to_string(),
            feed_ws_url: "wss://x".to_string(),
            l1_ws_url: "wss://x".to_string(),
            l1_http_url: url::Url::parse("https://l1.example.com/").unwrap(),
            anytrust_endpoint: "http://x".to_string(),
            starting_hotshot_height: 0,
            is_fresh_deployment: true,
            operator_private_key: Some("0xfromsecret".to_string()),
        };
        let key = resolve_operator_private_key(Some(&overrides)).unwrap();
        assert_eq!(key, "0xfromsecret");
    }

    #[test]
    fn apply_and_assert_replaces_all_placeholders() {
        let mut cfg: ServiceConfig<NitroConfig> =
            serde_json::from_str(placeholder_config_json()).unwrap();

        assert!(assert_no_placeholders_nitro(&cfg).is_err());

        let overrides = parse_secret_overrides(sample_secret_json()).unwrap();
        apply_overrides_nitro(&mut cfg, &overrides).unwrap();

        assert_eq!(
            cfg.espresso_client.client.base_url.as_str(),
            "https://query.example.com/"
        );
        assert_eq!(
            cfg.rollup.stack.feed.web_socket_url,
            "wss://feed.example.com/feed"
        );
        assert_eq!(cfg.rollup.stack.l1_ws_url, "wss://l1.example.com");
        assert_eq!(cfg.rollup.stack.l1_http_url, "https://l1.example.com/");

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

        assert_eq!(cfg.streamer.starting_hotshot_height, 9199257);
        assert!(cfg.is_fresh_deployment);

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
    fn assert_fails_when_l1_http_url_left_as_placeholder() {
        let mut cfg: ServiceConfig<NitroConfig> =
            serde_json::from_str(placeholder_config_json()).unwrap();
        // Apply only feed/ws/anytrust overrides; leave l1_http_url at PLACEHOLDER.
        cfg.espresso_client.client.base_url =
            url::Url::parse("https://query.example.com/").unwrap();
        cfg.rollup.stack.feed.web_socket_url = "wss://feed.example.com/feed".to_string();
        cfg.rollup.stack.l1_ws_url = "wss://l1.example.com".to_string();
        for provider in cfg.da_server.da_providers.iter_mut() {
            if provider.is_anytrust {
                provider.endpoint_url = "http://anytrust.example.com:9876".to_string();
            }
        }

        let err = assert_no_placeholders_nitro(&cfg).unwrap_err();
        assert!(err.to_string().contains("l1_http_url"));
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

        let overrides = fetch_secret_overrides(TeeType::Nitro)
            .await
            .expect("fetch_secret_overrides failed — check AWS_REGION, AWS_SECRET_ID, credentials, and secret JSON shape")
            .expect("AWS_REGION and AWS_SECRET_ID must both be set to run the live integration test");

        assert!(
            !overrides.espresso_base_url.is_empty(),
            "espresso_base_url is empty"
        );
        assert!(!overrides.feed_ws_url.is_empty(), "feed_ws_url is empty");
        assert!(!overrides.l1_ws_url.is_empty(), "l1_ws_url is empty");
        assert!(
            !overrides.l1_http_url.as_str().is_empty(),
            "l1_http_url is empty"
        );
        assert!(
            !overrides.anytrust_endpoint.is_empty(),
            "anytrust_endpoint is empty"
        );

        url::Url::parse(&overrides.espresso_base_url)
            .expect("espresso_base_url is not a valid URL");

        println!(
            "fetched secret with fields: espresso_base_url, feed_ws_url, l1_ws_url, l1_http_url, anytrust_endpoint, starting_hotshot_height={}, is_fresh_deployment={}",
            overrides.starting_hotshot_height, overrides.is_fresh_deployment
        );
    }
}
