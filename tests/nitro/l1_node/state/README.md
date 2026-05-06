# Persisted L1 state for the nitro test harness

This directory holds an Anvil L1 snapshot plus the contract artifacts that
`nitro-testnode` produced when those contracts were deployed against it.
Reusing the snapshot lets the e2e test skip the full L1 contract deployment
on every run, which dominates testnode startup time.

## Files

- `anvil-state.json` — output of `anvil_dumpState` after the testnode
  finished its full bootstrap (rollup + bridge contracts deployed).
- `deployed_chain_info.json` / `deployment.json` — the chain-info /
  deployment artifacts that `rollupcreator` would normally produce on a
  fresh run. Saved so the wrapper at
  [`rollupcreator-wrapper.sh`](../rollupcreator-wrapper.sh) can short-circuit
  the deploy step and copy them in.

## How it was generated

`setup.sh` orchestrates the bootstrap. From the repo root:

```bash
git submodule update --init --recursive
./tests/nitro/l1_node/state/setup.sh           # reuses snapshot if present
./tests/nitro/l1_node/state/setup.sh --init-force  # wipes & re-bootstraps
```

What it does:

1. Builds the custom `nitro-l1-anvil` image from
   [`anvil-l1.Dockerfile`](../anvil-l1.Dockerfile).
2. Writes `nitro-testnode/docker-compose.override.yml` in **bootstrap** mode
   (via [`write-override.sh`](../../write-override.sh)) so geth is replaced
   by Anvil and `rollupcreator` runs its normal deploy path.
3. Runs `./test-node.bash --no-simple --detach --init-force` inside
   `nitro-testnode`, waits for the sequencer RPC to come up.
4. Dumps Anvil state with `anvil_dumpState` into `anvil-state.json` and
   copies the rollupcreator artifacts out of the container.
5. Rewrites the override in **reuse** mode so subsequent runs load the
   snapshot and short-circuit deployment.
6. Tears down the testnode.

The e2e test harness (`tests/nitro/cas_harness.rs`) calls `write-override.sh
reuse` itself, so once the snapshot exists you don't run `setup.sh` again
unless the contracts/genesis change.

## Effect on testnode startup

Bootstrapping the rollup + bridge contracts from scratch takes roughly
8–10 minutes on a typical dev machine. With the snapshot loaded, the
`test-node.bash` init phase finishes in ~30–60 seconds (image pulls and
container startup dominate; no contract deployment).

## When to regenerate

Re-run with `--init-force` whenever any of these change:

- The Nitro contract code in the testnode submodule.
- The L2 genesis or chain info.
- The set of services brought up by `test-node.bash` that need on-chain
  state.
