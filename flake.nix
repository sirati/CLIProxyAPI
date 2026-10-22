{
  description = "sirati's fork of CLIProxyAPI: package, overlay, home-manager module, dev shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # Only the VM check uses it; consumers can make it follow theirs.
    home-manager = {
      url = "github:nix-community/home-manager";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      home-manager,
      ...
    }:
    let
      systems = [
        "aarch64-linux"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      dev = import ./devel.nix { inherit nixpkgs; };
      mkPackage =
        pkgs:
        pkgs.callPackage ./package.nix {
          src = self;
          rev = self.shortRev or self.dirtyShortRev or "dirty";
        };
    in
    dev
    // {
      packages = forAllSystems (
        system:
        let
          cliproxyapi = mkPackage nixpkgs.legacyPackages.${system};
        in
        {
          inherit cliproxyapi;
          default = cliproxyapi;
          # Fake upstreams and client workload for the seamless-upgrade test.
          handoff-probe = cliproxyapi.overrideAttrs {
            pname = "cliproxyapi-handoff-probe";
            subPackages = [ "test/handoff/probe" ];
            postInstall = "mv $out/bin/probe $out/bin/handoff-probe";
            doInstallCheck = false;
            meta.mainProgram = "handoff-probe";
          };
        }
      );

      apps = forAllSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.cliproxyapi}/bin/cliproxyapi";
        };
      });

      # Replaces pkgs.cliproxyapi with this fork, so nixpkgs' own module/tests
      # and anything depending on `pkgs.cliproxyapi` pick it up unchanged.
      overlays.default = final: _prev: {
        cliproxyapi = mkPackage final;
      };

      # services.cliproxyapi: the user service with seamless upgrades.
      homeManagerModules.default = import ./nix/hm-module.nix { inherit self; };

      checks.x86_64-linux.seamless-upgrade = import ./nix/seamless-upgrade-test.nix {
        pkgs = nixpkgs.legacyPackages.x86_64-linux;
        inherit self home-manager;
      };
    };
}
