# Light client technical reference

This reference maps the verified read path to source files, configuration and node APIs. It describes espresso-network revision `02794853a06`, pinned in `Cargo.toml`.

Start with the [overview](light-client-integration.md) for CAS's role. See the [catch-up guide](light-client-catchup.md) for genesis parameters and verification tests, or [image builds](image-builds.md) for build and deployment steps.

## Code entry points

| File / symbol | Responsibility |
| --- | --- |
| [`src/main.rs::build_reader`](../src/main.rs) | Selects the reader and constructs its query URLs, genesis and cache options |
| [`EspressoReader` and `LightClientEspressoReader`](../src/espresso_client/light_client.rs) | Read interface and adapter around the upstream verifier |
| [`LightClientConfig`](../src/config.rs) | Deserialized settings under `espresso_client.light_client` |
| [`src/streamer/streamer.rs`](../src/streamer/streamer.rs) | Polling, verified namespace reads and stub promotion |
| [`SecretOverrides`](../src/secrets.rs) | Supported runtime overrides; does not replace genesis |

## Verification guarantees

The default reader verifies data using the configured genesis, subject to the [historical Decaf assumption](light-client-catchup.md#historical-decaf-compatibility-assumption).

### Verified properties

- **Block finality:** checks quorum-certificate signatures against the epoch's validator set.
- **Header inclusion:** checks that a header belongs to a finalized block.
- **Payload integrity:** checks block data against the cryptographic summary in its header (the VID commitment).
- **Namespace contents:** checks the returned transactions, including proofs of an empty namespace.
- **Validator changes:** checks reconstructed stake tables against commitments in verified headers.

### Limitations

- A query node can delay or withhold data, preventing progress.
- `block_height()` can lag behind the network; it reports only the count the client has verified.
- Verification depends on a correctly configured genesis.

## Genesis configuration

`LightClientConfig.genesis` is an upstream `light_client::state::Genesis`. It is required configuration, including `chain_id`, and must match the Espresso network being read.

The [genesis guide](light-client-catchup.md#genesis-configuration-and-network-identity) describes its fields, Mainnet/Decaf values, source files and compatibility assumptions. It also explains how to validate the JSON used by the enclave builder.

## Client layers

```text
LightClient<P, S>
├─ P: Storage                  Local verified-state cache
└─ S: Client                   Node access
   ├─ QueryServiceClient       HTTP requests to one node
   └─ FallbackClient<T>        Requests across configured nodes
```

- `Client` is the node-access interface. It returns data and proofs without verifying them.
- `QueryServiceClient::new(url)` creates an HTTP client using `surf_disco` and the [node API routes](#node-api-routes).
- `FallbackClient` queries the primary node and alternatives below the verifier; request timing and URLs are specified in [CAS configuration](#cas-adapter-and-configuration).
- `Storage` is the verified-state cache interface. CAS uses `SqliteStorage`; persistence is configured in [CAS configuration](#cas-adapter-and-configuration).
- `LightClient<P, S>` combines node access, verified-state storage and proof verification.

## Node API routes

The pinned `QueryServiceClient` calls the routes below. Which routes are used for a read is an implementation detail of the light-client crate; CAS calls the high-level verifier.

| Purpose | HTTP path |
| --- | --- |
| Known block count | `GET /node/block-height` |
| Leaf finality proof | `GET /light-client/leaf/{id}` (also hash-based IDs and an optional finalized hint) |
| Raw leaf range / ranges | `GET /availability/leaf/{start}/{end}`, `POST /availability/leaf/ranges` |
| Header inclusion proof | `GET /light-client/header/{root}/{id}` |
| Payload proof / range / ranges | `GET /light-client/payload/{height}`, `GET /light-client/payload/{start}/{end}`, `POST /light-client/payload/ranges` |
| Namespace proof / range | `GET /light-client/namespace/{height}/{ns}`, `GET /light-client/namespace/{start}/{end}/{ns}` |
| Multiple namespaces | `GET /light-client/namespaces/{start}/{end}/{encoded}` |
| Stake-table events | `GET /light-client/stake-table/{epoch}` |
| Certificate | `GET /availability/cert2/{height}` |

The node must enable its `/light-client` API module. The dev-node implementation at the pinned revision enables it; CAS's dev-node Compose files use the `main` image. These are moving test images, so record the resolved image digest when diagnosing a regression.

## CAS client methods

CAS uses two methods on the verified reader:

| `LightClient` method | Returns |
| --- | --- |
| `block_height()` | Verified `u64` block count; may lag behind the network. The newest block number is count minus one. |
| `fetch_namespaces_in_range(start, end, ns)` | Verified `Vec<Vec<Transaction>>`, with one inner vector per height in `[start, end)` |

The submitter uses the query-service `EspressoClient` for these operations:

| Method | Purpose |
| --- | --- |
| `submit_transaction` → `POST /submit/submit` | Submit transactions; the light client has no write API |
| `fetch_transaction_by_hash` → `GET /availability/transaction/hash/...` | Confirm inclusion of the submitter's own transactions |
| `fetch_limits` | Query node limits; CAS bounds read batches with `HOTSHOT_RANGE_LIMIT = 100` |

## Verified read flow

The [CAS reader](../src/espresso_client/light_client.rs) delegates namespace reads to [`LightClient::fetch_namespaces_in_range`](https://github.com/EspressoSystems/espresso-network/blob/02794853a06870f1b9100be57c13fcebef5a3f69/crates/light-client/src/state.rs#L772). See that implementation for header verification, namespace-proof checks and the returned transactions. The link points to the revision pinned by CAS.

Ranges use an exclusive end: `[start, end)`. Since `block_height()` returns a count, the [streamer](../src/streamer/streamer.rs) reads `[from_block, count)` and waits when `from_block >= count`. For how the client verifies changes to the validator set, see the [catch-up guide](light-client-catchup.md#catch-up-verification).

## CAS adapter and configuration

The adapter wraps `Arc<LightClient<SqliteStorage, FallbackClient<QueryServiceClient>>>`. It returns namespace transactions directly, preserving empty blocks so positions still correspond to heights. The streamer uses this reader for polling and for loading queued entries whose full contents were deferred (stubs). It retries fetch or verification failures with increasing delays.

[`LightClientConfig`](../src/config.rs) stores these settings under `espresso_client.light_client`:

| Field | Meaning / default |
| --- | --- |
| `genesis` | Required initial network configuration; [fields and network values](light-client-catchup.md#genesis-configuration-and-network-identity) |
| `fallback_query_urls` | Additional read-only query nodes; empty by default |
| `db_path` | SQLite cache path; absent means in-memory |
| `num_stake_tables_in_memory` | In-memory table limit; default `100` |
| `fallback_delay_ms` | Delay before starting another fallback request; default `300` |

A persistent cache can retain verified state across restarts if its storage survives the container/enclave restart. An in-memory cache must rebuild that state. Fallback URLs serve verified reads; the submitter continues to use the primary `espresso_client.base_url`.

[`SecretOverrides`](../src/secrets.rs) applies supported runtime endpoints and starting-height settings. It does not replace genesis, and normal Nitro enclave startup rejects an empty genesis stake table.

The default build checks data against Espresso consensus. The `unverified-reader` compile feature produces a binary that trusts query responses, with a different enclave measurement (the hash identifying the enclave image). The `CAS_E2E_UNVERIFIED_READER` environment switch exists only in `e2e` builds; it is not a production runtime switch. See [image builds](image-builds.md) for version selection.

## Tests and diagnostics

The reader tests live in [`src/espresso_client/light_client.rs::light_client_tests`](../src/espresso_client/light_client.rs). `verifies_namespace_content_isolation_and_absence` exercises verified namespace content, isolation and absence against the Docker dev node.

The [genesis guide](light-client-catchup.md#validating-the-committed-genesis-files) contains the network-specific regression cases and runnable commands for the committed Mainnet/Decaf files. These live checks are ignored in ordinary CI. Its historical log section contains shortened examples from earlier experiments.

Queue tests can substitute `UnverifiedEspressoReader` to exercise streamer behavior without consensus proofs. See [CAS configuration](#cas-adapter-and-configuration) for reader selection and the [README](../README.md#log-verbosity) for log-filter precedence.
