use std::process::{Command, Stdio};

const WRITE_OVERRIDE_SCRIPT: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/nitro/write-override.sh");

/// Writes `nitro-testnode/docker-compose.override.yml` in `reuse` mode so that:
///   - geth is replaced by our anvil L1 image, which loads the persisted
///     state from `tests/nitro/l1_node/state/anvil-state.json` on startup.
///   - rollupcreator short-circuits `create-rollup-testnode` by copying the
///     saved deployment artifacts from the same directory.
///
/// Tests that need to redirect the poster to a CAS instance should call
/// [`setup_l1_reuse_mode_with_cas_poster`] instead.
pub fn setup_l1_reuse_mode() {
    run_write_override(&[]);
}

/// Same as [`setup_l1_reuse_mode`] but additionally rewrites the `poster`
/// service so it subscribes to the given CAS feed and uses the given CAS
/// DA RPC for batch data.
pub fn setup_l1_reuse_mode_with_cas_poster(feed_url: &str, calldata_rpc_url: &str) {
    run_write_override(&[
        ("CAS_FEED_URL", feed_url),
        ("CAS_CALLDATA_RPC_URL", calldata_rpc_url),
    ]);
}

fn run_write_override(envs: &[(&str, &str)]) {
    let mut cmd = Command::new("bash");
    cmd.arg(WRITE_OVERRIDE_SCRIPT)
        .arg("reuse")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let status = cmd.status().expect("failed to run write-override.sh");
    assert!(status.success(), "write-override.sh reuse failed");
}
