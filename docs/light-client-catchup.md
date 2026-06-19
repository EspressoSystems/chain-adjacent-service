# Stake-Table Catch-up & Verification — Visual Guide

> What "catch-up" actually is, what the pieces (leaf, stake table, epoch, genesis) mean,
> which chain we test against, where the genesis comes from and its format, and annotated
> **real success and failure logs** from the light client doing the work.

---

## 1. The setting: which chain, and the cast

We verify against **Espresso mainnet** (`https://query.main.net.espresso.network`), a live
network running **HotShot consensus** (a BFT protocol from the HotStuff lineage). A set of
**validators** vote on and finalize blocks. We (CAS, via the `light-client` crate) are a
**read-only verifier**: we fetch data + proofs from one (untrusted) query node and check the
validators' signatures ourselves.

### Definitions

| Term | What it means here |
|---|---|
| **Leaf** | One finalized block in the HotShot chain. Carries the block **header** and a **quorum certificate** (the validators' signatures finalizing it). "Leaf at height N" = block N. |
| **Quorum Certificate (QC)** | A bundle of validator **BLS signatures** proving a supermajority (by stake) finalized a leaf. Verifying a QC = checking those aggregated signatures against the validator set. |
| **Stake table** | The validator set for a given epoch: each validator's BLS public key + stake weight. The QC is checked *against this table*. Wrong table → signatures don't verify. |
| **Epoch** | A fixed span of `epoch_height` blocks over which **one** stake table applies. Mainnet `epoch_height = 40_000`. Epoch numbers increment as the chain grows. |
| **Genesis** | The trusted bootstrap: `epoch_height`, `first_epoch_with_dynamic_stake_table`, and the **initial stake table**. This is the *root of trust* — supplied out-of-band, not fetched from the untrusted node. |
| **Catch-up** | Rebuilding the stake table for the epoch you care about, starting from genesis, by replaying per-epoch stake changes and verifying each against consensus. |

---

## 2. Why catch-up exists

The stake table **changes over time** (validators join/leave/re-stake — "Proof of Stake").
To verify a leaf in epoch *N*, you need epoch *N*'s stake table. But you only *trust* the
**genesis** stake table. So you must walk forward from genesis to *N*, verifying each
epoch's table as you go. That walk is **catch-up**.

```
 epochs:   ... 274   275   276 │ 277   278   279   ...   N
                               │
           ── genesis table ───┤── dynamic (must be reconstructed) ──────▶
        (used directly, no     │  first_epoch_with_dynamic_stake_table = 277
         reconstruction)       │
```

- Epochs **< `first_epoch_with_dynamic_stake_table`** (here 277): just use the genesis table
  directly. No work.
- Epochs **≥ 277**: each must be **reconstructed and verified** from the previous one.

---

## 3. What catch-up does, step by step

To verify a leaf at target height *H* (epoch *N*):

```
  ┌─────────────────────────────────────────────────────────────────────────┐
  │ START from the last trusted table (genesis, or a cached/DB table ≤ N)     │
  └─────────────────────────────────────────────────────────────────────────┘
                                   │
                 for each epoch e from (start) up to N:
                                   │
        ┌──────────────────────────▼───────────────────────────┐
        │ 1. fetch stake-table EVENTS for epoch e                │  GET /light-client/stake-table/{e}
        │    (joins / exits / stake changes that happened)       │
        │ 2. REPLAY them onto the previous epoch's table         │  → candidate table for epoch e
        │ 3. fetch + verify the epoch-ROOT HEADER of epoch e-1   │  (certified by the previous,
        │    (the PREVIOUS epoch's root header)                  │   already-trusted stake table)
        │ 4. CHECK: hash(candidate table for e) ==               │  ← epoch e-1's root header commits
        │           that header's next_stake_table_hash          │     to epoch e's table ("next")
        │    mismatch → ERROR (reject); match → table is trusted │
        │ 5. cache the verified table (memory + optional SQLite) │

        Note: all of steps 1–5 run CLIENT-SIDE, in our process (the light-client crate).
        The node only serves raw, untrusted events/headers; it performs no verification.
        └──────────────────────────┬─────────────────────────────┘
                                   │
                ┌──────────────────▼───────────────────┐
                │ Now we have epoch N's trusted table.  │
                │ Verify the target leaf's QC against   │  ← BLS signature / pairing check
                │ it (finality). Then verify the        │
                │ namespace proof against the verified  │
                │ header.                               │
                └───────────────────────────────────────┘
```

Two distinct cryptographic checks are happening:
- **Per-epoch table check (step 4):** the reconstructed table must match a hash that
  consensus already signed into the epoch-root header. You can't forge the validator set.
- **Leaf finality check (last step):** the target leaf's QC signatures must verify against
  that table. You can't forge finalization.

**Cost is wall-clock, not money:** ~0.6 s/epoch. Measured against mainnet:

| Target | Epochs caught up | Time |
|---|---|---|
| first dynamic epoch | ~2 | ~1 s |
| ~50 epochs in | ~50 | 31 s |
| near live tip (~16.28M) | ~132 | 84 s |

In production the streamer polls forward, so it pays this **once** at startup to reach its
start height, then cached tables make subsequent polls cheap — *if* the SQLite cache is
persisted (`LIGHT_CLIENT_DB_PATH`); otherwise it re-pays on every restart.

---

## 4. Where the genesis comes from, and its format

The genesis is the **root of trust** and must match what honest nodes use. Two parts:

**(a) The values.** Each network publishes a genesis. For decaf, the canonical
light-client genesis is `light-client-query-service/genesis/decaf.toml` in espresso-network.
Format (TOML):

```toml
epoch_height = 3000
first_epoch_with_dynamic_stake_table = 1056

[[stake_table]]
stake_key = "BLS_VER_KEY~VbTfoVdZmeJU…"   # validator BLS public key
stake_amount = "0x1"
[[stake_table]]
stake_key = "BLS_VER_KEY~tK46FjhkzJrb…"
stake_amount = "0x1"
# … one block per validator
```

We convert that to the JSON our `LightClientConfig.genesis` deserializes (the `Genesis`
struct = `{ epoch_height, first_epoch_with_dynamic_stake_table, stake_table: [{stake_key,
stake_amount}, …] }`). Stored as test fixtures under `tests/fixtures/*.json`.

**(b) Sourcing the stake table when there's no published file** (mainnet has no
`light-client-query-service/genesis/mainnet.toml`). We validated two derivations:
- **Stake table** = a node's `GET /config/hotshot` → `config.known_nodes_with_stake[].stake_table_entry`
  (`stake_key` + `stake_amount`). Validated by confirming decaf's `/config/hotshot` equals
  its *published* genesis exactly.
- **`first_epoch_with_dynamic_stake_table`** = `epoch_start_block / epoch_height + 3`
  (reproduces decaf's published `1056`; mainnet → `277`, verified by a successful run).

> In production, genesis is delivered to CAS as trusted config via the enclave secrets path
> (the same channel as the query-node URL) — not fetched from the untrusted node.

---

## 5. Annotated SUCCESS log (real, ~10-epoch catch-up to block 11,360,201)

Command: `MAINNET_SMOKE_START=11360201 cargo test … live_smoke::mainnet_verifies_namespace_range -- --ignored --nocapture`

```
INFO  light_client::state: performing stake table catchup from=276 to=285
        └─ target leaf is in epoch 285; start from 276 (just below first dynamic epoch 277)

DEBUG light_client::state: reconstruct stake table epoch=277 num_events=2279
        └─ epoch 277 is the FIRST dynamic epoch → the big initial PoS bootstrap (2279 events)
DEBUG …stake_table: Filtered out invalid validators total_validators=34 filtered=34
        └─ CLIQUENET active-set filtering (validators missing p2p data are dropped)

DEBUG light_client::state: reconstruct stake table epoch=278 num_events=25
DEBUG light_client::state: reconstruct stake table epoch=279 num_events=17
DEBUG light_client::state: reconstruct stake table epoch=280 num_events=15
        └─ later epochs: only the deltas since the previous epoch (few events each)
…
DEBUG light_client::state: reconstruct stake table epoch=285 num_events=7
DEBUG light_client::state: found stake table in cache epoch=285
        └─ target epoch's table is now built + cached

MAINNET verified [11360201, 11360204) ns=1: 3 blocks, 0 txs total
        └─ leaf finality + namespace proofs verified against the epoch-285 table → 3 verified blocks
test result: ok. 1 passed … finished in 9.42s
```

Each `reconstruct stake table epoch=N` line is one iteration of the loop in §3: fetch events
→ replay → verify against the epoch-root header → cache. The chain of them from 277→285 *is*
the catch-up.

---

## 6. Annotated FAILURE log (wrong stake table → rejected)

Command: `cargo test … live_smoke::mainnet_rejects_wrong_stake_table -- --ignored --nocapture`
(mainnet epoch params, but the **wrong validators** substituted as the genesis stake table)

```
DEBUG light_client::consensus::quorum: verify QC chain for leaf version=0.4 leaf=Leaf2 {
        view_number: …, justify_qc: …, epoch: Some(EpochNumber(275)), block_number: 10960301,
        … next_stake_table_hash: None … }
        └─ it reaches the QC-verification step for a genesis-regime leaf (epoch 275 < 277,
           so it uses the GENESIS table directly — no catch-up needed to fail)

wrong-stake-table fetch result: Err(verifying QC
    0: invalid threshold signature
    1: Signature check failed: Verification failed, Pairing check failed)
        └─ the leaf's real QC was signed by MAINNET validators; checked against the wrong keys
           the BLS pairing equation does not hold → REJECTED
test result: ok. 1 passed … finished in 0.55s
```

This is the proof verification is real, not a rubber stamp:
- With the **correct** genesis → `verified … 3 blocks` (§5).
- With a **wrong** genesis → `Pairing check failed`, fast rejection (0.55 s), before it even
  starts catch-up — because the very first leaf's QC is checked against the (wrong) genesis
  table and fails immediately.

A malicious query node therefore cannot make CAS accept data: the data is only as trusted as
the genesis we configured, and everything else is checked against it cryptographically.

---

## 7. One-line mental model

> Catch-up = "walk the validator set forward from a trusted genesis to the epoch I care
> about, proving each step against consensus, so I can then check that block's finality
> signatures and namespace contents — and reject anything that doesn't add up."
