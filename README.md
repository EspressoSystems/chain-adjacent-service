# Chain Adjacent Service

The Chain Adjacent Service (CAS) is a middleware that integrates rollups with the [Espresso](https://www.espressosys.com/) Network. It handles both directions of the data flow:

- **Inbound**: pulls transactions from the Espresso network, filtered by rollup namespace, and makes them available to the rollup.
- **Outbound**: batches rollup messages, submits them to the Espresso Network, and tracks finalization against L1.

CAS exposes a JSON-RPC DA provider API that plugs directly into the rollup's existing DA interface, meaning the rollup does not need to be aware of Espresso internals. The rollup-specific logic (batch parsing, message types, L1 monitoring, verification) is isolated behind a `Rollup` trait, making it straightforward to add support for new rollup stacks. Arbitrum Nitro is the first supported rollup.

## Development

### Prerequisites

[Nix](https://nixos.org/download/) with flakes enabled.

### Enter the development shell

```bash
nix develop
```

### Initial setup


The E2E tests require Go, Docker. On a fresh machine:

```bash
just setup       # initialize git submodules
just check       # verify all prerequisites are in place
```

### Run the all tests

```bash
just test
```


## License
Copyright
(c) 2022 Espresso Systems espresso-network was developed by Espresso Systems. While we plan to adopt an open source license, we have not yet selected one. As such, all rights are reserved for the time being. Please reach out to us if you have thoughts on licensing.

## Disclaimer
DISCLAIMER: This software is provided "as is" and its security has not been externally audited. Use at your own risk.

DISCLAIMER: The Rust library crates provided in this repository are intended primarily for use by the binary targets in this repository. We make no guarantees of public API stability. If you are building on these crates, reach out by opening an issue to discuss the APIs you need.