# Light Client Integration — Technical Reference

> Companion to `light-client-integration.md` (the plain-English onboarding doc). This one
> is the deep version: what the light client actually does, every API call involved, what
> CAS calls, the exact verified read path, our integration, and what we proved against live
> networks. Written against espresso-network rev `0d10e7f` (2026-02-11), the rev CAS pins.

---

## 1. The problem and the goal

CAS consumes our rollup's transactions out of Espresso and feeds them to the rollup. Until
now it did this through the Espresso **query service** (the `availability`/`status` HTTP
API) and **trusted whatever the server returned**. A buggy, compromised, or malicious query
node could feed CAS:

- transactions that were never finalized,
- fabricated transactions,
- a tampered view of a block's contents.

CAS would forward all of it to the rollup as truth. Notably, the query service already
returned a namespace-inclusion proof (`NamespaceTransactionsInRange.proof`) — and CAS
**threw it away unchecked**. So the trust was total and blind.

**Goal:** stop trusting the query node for the data we consume. Verify it instead.

---

## 2. What the light client is

`light-client` is a read-only **verifier** crate from espresso-network. You point it at one
or more *untrusted* query nodes; it fetches data **plus cryptographic proofs**, verifies
everything against HotShot consensus, and only returns data that checks out. A malicious
node cannot make it accept a non-finalized block, a forged header, a tampered payload, or
the wrong namespace contents — those all error.

It is "light" because it does not run consensus or store the chain; it only verifies proofs.

### What it verifies (and what it doesn't)

| Verifies | Does NOT prevent |
|---|---|
| **Leaf finality** — the block was finalized by HotShot (quorum-certificate signatures checked against the correct epoch's stake table) | **Liveness / withholding** — a node can stall or refuse data |
| **Header inclusion** — the header belongs to a finalized leaf | **Staleness** — `block_height()` may *underestimate* (it never reports a height it hasn't verified) |
| **Payload integrity** — VID commitment matches the header | **Genesis misconfiguration** — if the root-of-trust genesis is wrong, all bets are off |
| **Namespace transactions** — the returned txs really are that block's namespace contents (incl. proving *absence*) | |
| **Stake-table evolution** — each epoch's stake table reconstructed and checked against consensus-signed header commitments | |

---

## 3. The trust model: genesis, stake tables, epochs

HotShot validators sign what they finalize (a **quorum certificate** = supermajority of
signatures, weighted **by stake**). To check those signatures you must know the legitimate
validator set — the **stake table**. The stake table changes over time (PoS), so the light
client needs a trusted starting point.

### Genesis = the root of trust

```rust
// light_client::state::Genesis
pub struct Genesis {
    pub epoch_height: u64,                              // blocks per epoch
    pub first_epoch_with_dynamic_stake_table: EpochNumber, // before this, use the genesis table
    pub stake_table: Vec<StakeTableEntry<PubKey>>,     // the trusted initial validator set
    #[cfg(feature = "decaf")]
    pub decaf_first_pos_epoch: Option<EpochNumber>,    // decaf 0.2->0.4 transition carve-out
}
```

The doc comment is blunt: *"if the genesis is not correct (matching honest HotShot nodes)
the light client may verify incorrect data, or fail to verify correct data."* Genesis is
the one thing we must get right and supply out-of-band — it comes from the network's
`genesis.toml` (delivered to CAS via the enclave secrets path).

### How stake tables evolve (epochs)

- **Epochs `< first_epoch_with_dynamic_stake_table`** → use the genesis stake table directly.
- **Dynamic epochs** → the light client reconstructs each epoch's stake table by replaying
  per-epoch stake-table *events* fetched from the (untrusted) node, then **verifies the
  reconstructed table's hash against the consensus-signed epoch-root header**. It caches
  results in memory + the local DB.

**Cost implication:** verifying a leaf in epoch *N* may require catching up the stake table
from genesis to *N*, one epoch at a time. On a long-lived chain that cold-start is
expensive — which is why a persistent DB (`SqliteStorage` with `LIGHT_CLIENT_DB_PATH`) and
the fact that recent blocks dominate matter.

---

## 4. The three layers of the crate

```
   LightClient<P, S>              ← the VERIFIER. The only thing re-exported at crate root.
     ├─ P: Storage                ← local verified-state cache (SqliteStorage provided)
     └─ S: Client                 ← how to talk to nodes (a trait)
            ├─ QueryServiceClient  ← HTTP client for ONE query node
            └─ FallbackClient<T>   ← rotates over several clients for liveness (NOT at our rev)
```

- **`Client`** (trait) — raw, *unverified* node access. Returns proofs; verifies nothing.
- **`QueryServiceClient::new(url)`** — HTTP impl over `surf_disco`, hits the paths in §5.
- **`FallbackClient`** — ordered failover across nodes (liveness, not trust). **Does not
  exist at rev `0d10e7f`** — added later on `main`. We use a single `QueryServiceClient`
  now, kept generic so `FallbackClient` drops in after a future rev bump.
- **`Storage`** (trait) — verified-state cache. `SqliteStorage` is provided
  (`LIGHT_CLIENT_DB_PATH`, else in-memory and rebuilt via catch-up each start).
- **`LightClient<P,S>`** — ties them together and does all verification.

---

## 5. The API surface — every endpoint involved

`QueryServiceClient` (the `Client` impl) calls these node endpoints. This is the complete
list, with what each is for and whether the **stock dev node** serves it:

| `Client` method | HTTP path | Purpose | Dev node? | Real nodes? |
|---|---|---|---|---|
| `block_height` | `GET /node/block-height` | latest known height | ✅ | ✅ |
| `get_leaves_in_range` | `GET /availability/leaf/{start}/{end}` | raw leaves (incl. `justify_qc`) | ✅ | ✅ |
| `leaf_proof` | `GET /light-client/leaf/...` | finality proof for a leaf | ❌ | ✅ |
| `header_proof` | `GET /light-client/header/{root}/{id}` | Merkle inclusion proof | ❌ | ✅ |
| `payload_proof` | `GET /light-client/payload/{height}` | VID payload proof | ❌ | ✅ |
| `namespace_proof(s)_in_range` | `GET /light-client/namespace/{start}/{end}/{ns}` | namespace inclusion proofs | ❌ | ✅ |
| `stake_table_events` | `GET /light-client/stake-table/{epoch}` | per-epoch stake events (catch-up) | ❌ | ✅ |

**Key fact:** the `/light-client/*` endpoints are an **opt-in sequencer API module**
(`module!("light-client", ..., requires: "http", "storage-sql")`). A node serves them only
if its operator enables that module. The **stock `espresso-dev-node` never enables it** (its
`api_options` are `http + submit + config + explorer + query_sql + hotshot_events`, true at
our rev and current `main`). Real network nodes **do** enable it — verified live:
`GET /light-client/leaf/10 → 200` on both `query.main.net.espresso.network` and
`query.decaf.testnet.espresso.network`. The crate's own HTTP test enables the module on a
sequencer `TestNetwork` before connecting.

> The data isn't missing on the dev node — `/availability/leaf/...` returns leaves with
> their `justify_qc`, and the old availability namespace endpoint returned an `NsProof`. The
> `/light-client/*` module just *packages* that raw data into the proof types the verifier
> wants. Reimplementing that packaging ourselves to use the dev node would mean fighting the
> grain of the crate, so we don't.

---

## 6. What CAS calls (the small surface)

CAS only needs two things from the verifier, both on the inbound consumption path:

| `LightClient` method | Replaces (old `EspressoClient`) | Returns |
|---|---|---|
| `block_height()` | `fetch_latest_hotshot_block_height()` | `u64` (verified, may underestimate) |
| `fetch_namespaces_in_range(start, end, ns)` | `fetch_namespace_transactions_in_range(ns, start, end)` | `Vec<Vec<Transaction>>` (verified), one inner vec per height in `[start, end)` |

Everything else stays on the existing query-service `EspressoClient`, by design:

| Stays on `EspressoClient` | Why |
|---|---|
| `submit_transaction` → `POST /submit/submit` | a write; the verifier has no write path; not the query API anyway |
| `fetch_transaction_by_hash` → `GET /availability/transaction/hash/...` | only used by the submitter to confirm **our own** just-submitted tx landed — a low-stakes read, not third-party data we forward |
| `fetch_limits` | only bounded range size; we chunk ourselves via `HOTSHOT_RANGE_LIMIT = 100` |

So after integration CAS runs **two clients side by side**: `EspressoClient` for
submit/self-confirm, `LightClient` for the trustless read path.

---

## 7. The verified read path, step by step

What actually happens inside `fetch_namespaces_in_range(start, end, ns)` (the call our
streamer makes):

```
fetch_namespaces_in_range(start, end, ns)
│
├─ 1. fetch_headers_in_range(start, end)
│     └─ fetch_leaves_in_range(start, end)
│        ├─ try local DB; if it has all [start,end) leaves, use them (already verified)
│        ├─ else fetch the ANCHOR leaf (end-1) and fully verify its finality:
│        │     leaf_proof(end-1)  [GET /light-client/leaf/...]
│        │     → inspect proof.epoch():
│        │         Some(e) → build StakeTableQuorum for epoch e (catch-up if needed)
│        │                   and verify the quorum-certificate signatures
│        │         None    → require a locally-trusted finalized leaf (Assumption); else error
│        │     → ensure the proof is really for leaf (end-1), not a substitute
│        └─ fetch the rest [GET /availability/leaf/...] and CHAIN them to the anchor
│              by parent-hash (each leaf.hash() must equal the next's parent_commitment)
│        → headers ride this verified leaf path, so headers are now trusted
│
├─ 2. server.namespace_proofs_in_range(start, end, ns)   [GET /light-client/namespace/...]
│        (unverified proofs from the node)
│
├─ 3. ensure proofs.len() == headers.len()
│
└─ 4. for each (proof, header): proof.verify(header, ns)
         → checks the txs are that header's namespace contents (or proves absence)
         → returns Vec<Transaction> per block
   ⇒ Vec<Vec<Transaction>>   (fully verified)
```

The subtle part — and why the *original ticket's snippet was wrong*: you must verify the
**header** against consensus *first* (step 1), then verify the namespace proof against that
*verified* header (step 4). The ticket verified proofs against headers pulled raw from the
untrusted node — circular, proves nothing. `LightClient` does it in the right order; that's
why we use `LightClient`, not the bare `FallbackClient` the ticket linked.

`block_height()` is simpler: it asks the node for its height, and if that's higher than what
it has verified, it fetches+verifies that leaf to confirm the block exists. If verification
fails it falls back to the last verified height (hence "may underestimate").

---

## 8. Our integration (what we built)

```
                         ┌──────────────────────────── CAS ────────────────────────────┐
   Espresso  <── submit ─┤ EspressoClient ── submit_transaction, fetch_transaction_by_hash │
                         │                                                                │
   Espresso  ── reads ──>┤ LightClientReader (NEW) ── wraps Arc<LightClient<Sqlite,QSC>>   │
                         │      │  block_height()                                          │
                         │      │  namespace_transactions_in_range()                       │
                         │      ▼                                                          │
                         │   Streamer ── poll_hotshot_blocks / promote_stubs ── parse ──▶ rollup
                         └────────────────────────────────────────────────────────────────┘
```

**Files:**

- **`src/espresso_client/light_client.rs`** (new) — `LightClientReader<S = QueryServiceClient>`,
  wrapping `Arc<LightClient<SqliteStorage, S>>` (`LightClient` isn't `Clone`; the `Arc` makes
  the reader cheaply cloneable, which the streamer needs to hand a handle to its poll task).
  - `block_height()` → `LightClient::block_height()`
  - `namespace_transactions_in_range(ns, start, end)` → calls `fetch_namespaces_in_range`
    and adapts `Vec<Vec<Transaction>>` into the streamer's existing
    `Vec<NamespaceTransactionsInRange { transactions, proof: None }>` shape. `proof` is
    `None` because verification already happened inside the light client — nothing
    downstream ever read that field. **This adapter is the keystone that keeps the rest of
    the pipeline (parsing, queue, stubs) unchanged.**
  - Generic over `S: Client` so `FallbackClient<QueryServiceClient>` drops in later.

- **`src/config.rs`** — new `LightClientConfig { genesis: Genesis, db_path: Option<PathBuf> }`.
  The query-node URL **reuses `espresso_client.base_url`** (the same Espresso node serves
  both `/submit` and the read endpoints), so no new URL config.

- **`src/streamer/streamer.rs`** — the `client` field and the two read call sites
  (`poll_hotshot_blocks`, `promote_stubs`) now use `LightClientReader`. The existing
  exponential-backoff retry loop **is** the agreed failure mode: an honest node always
  progresses; a bad node's proofs fail to verify → retry (and rotate, once `FallbackClient`
  lands); total dishonesty → effectively halt-and-alert.

- **`src/main.rs`** — builds the reader from `config.light_client.genesis` +
  `espresso_client.base_url` + `db_path`, hands it to the streamer. Submitter unchanged.

**Tests:** 92 lib tests pass. Two dev-node *polling* tests are `#[ignore]`d (they need a node
that serves `/light-client` — see §10). Queue-logic tests use an in-memory reader
(`new_for_test`) and never poll.

---

## 9. What we proved against live networks

Tests live in `src/espresso_client/light_client.rs::live_smoke` (all `#[ignore]`d).

- **The path is correct.** Mainnet and decaf query nodes both serve the `/light-client`
  module and return real proof payloads. The stock dev node does *not* (fixed by PR #4453 —
  §10).
- **✅ Mainnet verifies end-to-end, incl. deep catch-up** (`mainnet_verifies_namespace_range`):
  fetches a verified namespace range against `query.main.net.espresso.network` — real
  stake-table catch-up + leaf finality + namespace-proof verification. Exercised across
  catch-up depths up to **~132 epochs** (near the live tip, ~84s) via `MAINNET_SMOKE_START`.
- **✅ Verification is genuine** (`mainnet_rejects_wrong_stake_table`): feeding mainnet's
  proofs the WRONG validators is **rejected** (`invalid threshold signature / Pairing check
  failed`). A no-op verifier would pass this; it errors.
- **✅ Local dev node** (`devnode_verifies_namespace_range`): verifies against the dockerized
  dev node (PR #4453 image). Genesis-regime only — the dev node can't reach a dynamic epoch
  under emulation (§10).

Two derivations we validated and rely on:
- **Genesis stake table** = a node's `GET /config/hotshot` → `config.known_nodes_with_stake`
  (decaf's matched its published genesis exactly; mainnet's + dev node's verified).
- **`first_epoch_with_dynamic_stake_table`** = `epoch_start_block / epoch_height + 3`
  (reproduces decaf's published `1056`; mainnet `277` verified).

Run: `cargo test -p chain-adjacent-service live_smoke -- --ignored --nocapture`.
Genesis fixtures: `tests/fixtures/{mainnet_genesis.json, dev_node_genesis.json}`.

---

## 10. Open items and limitations

| Item | Status / detail |
|---|---|
| **Dev node `/light-client` (PR #4453)** | The stock `espresso-dev-node` binary never enables the module (at our rev *and* `main`); we added `.light_client(...)` in PR EspressoSystems/espresso-network#4453. `docker-compose.yml` runs the PR-built image (`pr-4453`) locally — switch to a registry tag once it merges. Until then, the docker-based tests need that image loaded. |
| **No dynamic catch-up on the dev node** | Lowering `epoch_height` to force epoch transitions stalls the amd64-emulated dev node (DRB + stake-table reconstruction too slow). So the dev node only covers the genesis-regime path; **mainnet is the only practical dynamic-catch-up coverage**. (May work on native-Linux CI.) |
| **Decaf 0.2→0.4 transition (not tested in code)** | Decaf needs `Genesis.decaf_first_pos_epoch` (absent from the published genesis, no runtime flag at our rev) to verify past its first dynamic epoch. A decaf-testnet quirk — mainnet carries the hash, so production is unaffected. We removed the decaf test; the finding is kept here and in the catch-up doc. |
| **`FallbackClient` / multiple query URLs** | Not at rev `0d10e7f`. `LightClientReader` is concrete today; to fail over, generalize over `S: Client` and build `FallbackClient<QueryServiceClient>` in `new` after a rev bump. Config is single-URL (reuses `base_url`). |
| **Genesis delivery** | Currently in the config file; should be delivered via the enclave secrets path (same as `base_url`), sourced from the network's `genesis.toml`. |
| **Cold-start catch-up cost** | Set `LIGHT_CLIENT_DB_PATH` to persist the verified-state cache across enclave restarts, else catch-up is repaid every start. |

---

## 11. Glossary

- **HotShot** — Espresso's BFT consensus protocol (derived from the HotStuff paper).
- **Leaf** — a node in the consensus chain; carries a header and a quorum certificate.
- **Quorum certificate (QC)** — a supermajority of validator signatures finalizing a block.
- **Stake table** — the validator set and their stake weights for an epoch; what QCs are
  checked against.
- **Epoch** — a fixed span of blocks (`epoch_height`) over which a given stake table applies.
- **Namespace** — a rollup's lane within an Espresso block; CAS filters by its namespace.
- **NsProof / NamespaceProof** — proof that a set of txs is exactly a block's namespace
  contents (or that the namespace is absent).
- **Genesis** — the trusted initial config (stake table + epoch params) that roots all
  verification.
