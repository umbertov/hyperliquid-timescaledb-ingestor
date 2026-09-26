{
  description = "Development shell and Docker image for the Hyperliquid market-data collector";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.rust-overlay.url = "github:oxalica/rust-overlay";
  inputs.rust-overlay.inputs.nixpkgs.follows = "nixpkgs";

  outputs = { nixpkgs, rust-overlay, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ (import rust-overlay) ];
      };
      rustToolchain = pkgs.rust-bin.stable."1.98.1".minimal.override {
        extensions = [ "clippy" "rust-analyzer" "rust-src" "rustfmt" ];
      };
      rustPlatform = pkgs.makeRustPlatform {
        cargo = rustToolchain;
        rustc = rustToolchain;
      };
      rustSource = pkgs.lib.cleanSourceWith {
        src = ./.;
        filter = path: type:
          let
            root = toString ./.;
            value = toString path;
            relative = if value == root then "" else pkgs.lib.removePrefix "${root}/" value;
            source = builtins.any (dir: relative == dir || pkgs.lib.hasPrefix "${dir}/" relative)
              [ "src" "tests" "benches" "examples" ".cargo" ];
            buildFile = builtins.elem relative [ "Cargo.toml" "Cargo.lock" "build.rs" "rust-toolchain" "rust-toolchain.toml" ];
          in value == root || source || buildFile;
      };
      app = pkgs.pkgsStatic.rustPlatform.buildRustPackage {
        pname = "hyperliquid-timescaledb-collector";
        version = "0.1.0";
        src = rustSource;
        cargoLock.lockFile = ./Cargo.lock;
        nativeBuildInputs = [ pkgs.pkg-config pkgs.cmake pkgs.perl ];
        buildInputs = [ pkgs.pkgsStatic.openssl ];
      };
    in {
      formatter.${system} = pkgs.nixfmt-tree;
      packages.${system} = {
        default = app;
        docker = pkgs.dockerTools.buildLayeredImage {
          name = "hyperliquid-timescaledb-collector";
          tag = "latest";
          config = {
            Cmd = [ "${app}/bin/hyperliquid-timescaledb-collector" ];
            Env = [ "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt" ];
          };
          contents = [ pkgs.cacert ];
        };
      };
      apps.${system} = {
        up = {
          type = "app";
          program = "${pkgs.writeShellApplication {
            name = "hyperliquid-up";
            runtimeInputs = [ pkgs.docker pkgs.docker-compose pkgs.nix ];
            text = ''
              if [ ! -f "$PWD/flake.nix" ] || [ ! -f "$PWD/docker-compose.yml" ]; then
                echo "Run this command from the project root." >&2
                exit 1
              fi
              image="$(nix build .#docker --no-link --print-out-paths)"
              docker load --input "$image"
              exec docker-compose --project-directory "$PWD" -f "$PWD/docker-compose.yml" up -d "$@"
            '';
          }}/bin/hyperliquid-up";
        };
        compose = {
          type = "app";
          program = "${pkgs.writeShellApplication {
            name = "hyperliquid-compose";
            runtimeInputs = [ pkgs.docker-compose ];
            text = ''
              if [ ! -f "$PWD/docker-compose.yml" ]; then
                echo "Run this command from the project root." >&2
                exit 1
              fi
              exec docker-compose --project-directory "$PWD" -f "$PWD/docker-compose.yml" "$@"
            '';
          }}/bin/hyperliquid-compose";
        };
      };
      devShells.${system}.default = pkgs.mkShell {
        packages = [ pkgs.diesel-cli pkgs.cmake pkgs.git pkgs.openssh pkgs.openssl pkgs.perl
          pkgs.pkg-config pkgs.postgresql_18 pkgs.stdenv.cc pkgs.uv rustToolchain ];
        CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = "${pkgs.stdenv.cc}/bin/cc";
      };
    };
}
