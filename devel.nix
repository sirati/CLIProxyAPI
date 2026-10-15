{ nixpkgs }:
let
  systems = [
    "aarch64-linux"
    "x86_64-linux"
  ];
  forAllSystems = nixpkgs.lib.genAttrs systems;
in
{
  devShells = forAllSystems (
    system:
    let
      pkgs = import nixpkgs { inherit system; };
    in
    {
      default = pkgs.mkShell {
        packages = [
          pkgs.go
          pkgs.gopls
          pkgs.gotools
          pkgs.golangci-lint
          pkgs.delve
          pkgs.gnumake
        ];

        # Keep module/build caches out of $HOME's global state per checkout.
        shellHook = ''
          export GOFLAGS="-buildvcs=false"
        '';
      };
    }
  );
}
