# CAS images and enclave builds

CAS publishes an application image. `tee-image-builder` uses it with the chain configuration and Espresso genesis to build an AWS Nitro Enclave image (EIF). CAS does not reference a `tee-image-builder` image in its code or configuration.

## Choose the rollup's Nitro version at build time

| Target Nitro version | Cargo feature | Published CAS tag suffix |
| --- | --- | --- |
| 3.9.9 | `nitro-v3_9_9` | `-v3_9_9` |
| 3.10 | `nitro-v3_10` | `-v3_10` |
| 3.12.1 | `nitro-v3_12_1` | `-v3_12_1` |

The v3.9.9, v3.10 and v3.12.1 features are mutually exclusive. They select the feed-signature format and hashing scheme, along with supported AnyTrust headers. Choose the variant matching the rollup's Nitro protocol; changing runtime JSON or an environment variable cannot switch variants.

Cargo and Docker default to 3.9.9. To build a variant explicitly:

```bash
docker build --build-arg CARGO_FEATURES=nitro-v3_9_9 -t cas:v3_9_9 .
docker build --build-arg CARGO_FEATURES=nitro-v3_10 -t cas:v3_10 .
docker build --build-arg CARGO_FEATURES=nitro-v3_12_1 -t cas:v3_12_1 .
```

The Dockerfile's dependency-cache stage uses the 3.9.9 marker for all three builds because the markers have identical dependency trees. The final build uses `CARGO_FEATURES` and `--no-default-features`, so the cache stage does not determine the final variant.

`NITRO_IMAGE` in `e2e/nitro/.env` selects the Nitro node containers used by tests. It does not select the production CAS variant or the source image used by the enclave builder.

## Nitro v3.12.1

The v3.10 and v3.12.1 variants share feed-signature, batch-parsing and DA RPC formats. Each has an independent Cargo feature and image tag. The signature regression test uses the expected hash from [Nitro's Go test](https://github.com/OffchainLabs/nitro/blob/v3.12.1/broadcaster/message/message_signature_test.go).

Keep `node.feed.input.rest.enable=false` when consuming CAS's feed: CAS serves websocket messages and does not implement Nitro's REST backlog API. The `execution.transaction-feed` endpoint uses a separate transaction/receipt schema and is not CAS's signed sequencer-message feed.

## Publishing

[Docker Publish](../.github/workflows/docker-publish.yml) builds all three verified variants on pushes to `main`, release tags matching `v*`, and manual dispatch. There is no workflow input to choose just one Nitro version; the matrix builds all three.

The registry is `ghcr.io/espressosystems/chain-adjacent-service`. Tags include:

- `main-v3_9_9`, `main-v3_10` and `main-v3_12_1` for the main branch.
- `<published-sha>-v3_9_9`, `<published-sha>-v3_10` and `<published-sha>-v3_12_1` for the published short commit SHA.
- Release-version tags with the same suffixes.

Use an explicit published version when preparing an enclave deployment, and record the resolved image digest (its content hash) alongside its source revision and resulting PCR0, the measurement identifying the enclave image. Keep track of the actual published commit: a merge commit can differ from the PR head reviewed in CAS.

The existing `latest` tag policy stays on the v3.9.9 variant; v3.10 and v3.12.1 use explicit version suffixes.

The separate [unverified publish workflow](../.github/workflows/docker-publish-unverified.yml) builds all three variants only on manual dispatch and adds `unverified-reader` plus an `-unverified` tag suffix. Those images trust query responses and produce a distinct binary/enclave measurement; they are not the default verified images.

## Build the enclave in tee-image-builder

Run its [Build EIF Image (CAS) workflow](https://github.com/EspressoSystems/tee-image-builder/blob/main/.github/workflows/cas.yml) after the selected CAS application image has been published. Supply:

| Workflow input | What it selects |
| --- | --- |
| `cas_image_path` | Full CAS image reference, including the matching Nitro suffix; for example `ghcr.io/espressosystems/chain-adjacent-service:<published-sha>-v3_10` |
| `chain` | Rollup deployment config: `chain-configs/cas/<chain>.json` |
| `genesis` | Espresso genesis file: `chain-configs/cas/genesis/<genesis>.json`, such as `mainnet` or `decaf` |
| `aws_region`, `aws_secret_id` | Location of runtime endpoint/operator overrides |
| `enforce_config` | Whether to enforce the baked config hash; enabled for deployment |

Always supply `cas_image_path` explicitly rather than relying on the builder's fallback tag. Defaults are controlled in tee-image-builder; CAS publishes the suffixed tags above. Automatic push builds also need their deployment inputs configured. Publishing CAS alone does not deploy an enclave.

The builder passes `cas_image_path` as `SOURCE_IMAGE` to its Dockerfile. It does not recompile CAS or change the chosen Nitro version. It merges the chosen genesis into `espresso_client.light_client.genesis` before computing the config hash and including the configuration in the image.

Record the resulting EIF image reference, PCR0 and config hash. Register the new enclave hash on the appropriate TEE verifier and update the deployment's attestation and AWS Key Management Service (KMS) policy as required before running it. Then check startup, verified Espresso progress, batch posting and validator catch-up in the target test environment.

## Configuration changes after a build

Changing the application image, Nitro variant or baked genesis requires a new enclave build and measurement. The [genesis guide](light-client-catchup.md#genesis-configuration-and-network-identity) specifies the root-of-trust inputs. Runtime logging changes are described in the [README](../README.md#log-verbosity); they require a restart, not a new image.

For service connections and host networking, see [hosting CAS](cas-deployment-topology.md).
