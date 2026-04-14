{
  description = "Chain Adjacent Service";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        rustToolchain = pkgs.rust-bin.stable."1.93.1".default.override {
          extensions = [ "rust-src" "rustfmt" "clippy" ];
        };

        celestiaApp = pkgs.stdenv.mkDerivation rec {
          pname = "celestia-app";
          version = "4.1.0";
          src = pkgs.fetchurl {
            url = "https://github.com/celestiaorg/celestia-app/releases/download/v${version}/celestia-app_Linux_x86_64.tar.gz";
            hash = "sha256-eC48e71UBIt7TochX8wXurir33D60K0DvouSknt/R04=";
          };
          nativeBuildInputs = [ pkgs.autoPatchelfHook ];
          unpackPhase = "tar -xzf $src";
          installPhase = ''
            mkdir -p $out/bin
            install -m755 celestia-appd $out/bin/
          '';
        };

        celestiaNode = pkgs.stdenv.mkDerivation rec {
          pname = "celestia-node";
          version = "0.28.4";
          src = pkgs.fetchurl {
            url = "https://github.com/celestiaorg/celestia-node/releases/download/v${version}/celestia-node_Linux_x86_64.tar.gz";
            hash = "sha256-u4uf0qyFmUX98vjIS6A4914BvlTlbRqUbLvUtUDxINk=";
          };
          nativeBuildInputs = [ pkgs.autoPatchelfHook ];
          unpackPhase = "tar -xzf $src";
          installPhase = ''
            mkdir -p $out/bin
            install -m755 celestia $out/bin/
          '';
        };

        darwinDeps = pkgs.lib.optionals pkgs.stdenv.isDarwin (with pkgs; [
          libiconv
          darwin.apple_sdk.frameworks.Security
          darwin.apple_sdk.frameworks.SystemConfiguration
        ]);
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            openssl
            celestiaApp
            celestiaNode
          ] ++ darwinDeps;

          nativeBuildInputs = with pkgs; [
            pkg-config
            just
            rustToolchain
          ];

          RUSTFLAGS = "-Dwarnings";
          CARGO_TERM_COLOR = "always";
        };
      }
    );
}
