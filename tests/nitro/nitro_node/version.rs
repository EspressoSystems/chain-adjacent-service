// When adding a new Nitro version:
//   1. Add a variant here with the correct metadata.
//   2. Create `e2e/nitro/versions/<tag>/nitro-config/` with version-specific configs.
//   3. Run `just generate-l1-state <tag>` to populate `generated-config/`.
//   4. Add the test functions that reference the new version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NitroVersion {
    V3_9_9,
    V3_10_0,
}

impl NitroVersion {
    pub fn docker_image(self) -> &'static str {
        match self {
            Self::V3_9_9 => "offchainlabs/nitro-node:v3.9.9-6b0af88",
            Self::V3_10_0 => "offchainlabs/nitro-node:v3.10.0-b1cf6db",
        }
    }

    pub fn poster_image(self) -> &'static str {
        match self {
            Self::V3_9_9 => "ghcr.io/espressosystems/nitro-espresso-integration/nitro-node:pr-1052",
            Self::V3_10_0 => Self::V3_10_0.docker_image(),
        }
    }

    pub fn poster_platform(self) -> &'static str {
        match self {
            Self::V3_9_9 => "linux/amd64",
            Self::V3_10_0 => "",
        }
    }

    pub fn wasm_module_root(self) -> &'static str {
        match self {
            Self::V3_9_9 | Self::V3_10_0 => {
                "0xc2c02df561d4afaf9a1d6785f70098ec3874765c638e3cb6dbe8d3c83333e14c"
            }
        }
    }

    // V3_10_0 keeps the top-level paths for backward compat with `just e2e-up`.
    pub fn generated_config_dir(self) -> &'static str {
        match self {
            Self::V3_9_9 => "versions/v3.9.9/generated-config",
            Self::V3_10_0 => "generated-config",
        }
    }

    pub fn nitro_config_dir(self) -> &'static str {
        match self {
            Self::V3_9_9 => "versions/v3.9.9/nitro-config",
            Self::V3_10_0 => "nitro-config",
        }
    }

    // v3.9.9: `--node.da-provider.*` (patched image removes mutual exclusion check)
    // v3.10.0: `--node.da.external-provider.*`
    pub fn da_provider_flag_prefix(self) -> &'static str {
        match self {
            Self::V3_9_9 => "--node.da-provider",
            Self::V3_10_0 => "--node.da.external-provider",
        }
    }

    pub fn tag(self) -> &'static str {
        match self {
            Self::V3_9_9 => "v3.9.9",
            Self::V3_10_0 => "v3.10.0",
        }
    }

    pub fn anytrust_server_bin(self) -> &'static str {
        match self {
            Self::V3_9_9 => "/usr/local/bin/daserver",
            Self::V3_10_0 => "/usr/local/bin/anytrustserver",
        }
    }
}

impl Default for NitroVersion {
    fn default() -> Self {
        Self::V3_10_0
    }
}
