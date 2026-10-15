{
  description = "sirati's fork of CLIProxyAPI: package, overlay, dev shell";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs, ... }:
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
    };
}
