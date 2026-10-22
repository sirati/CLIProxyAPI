# Builds this checkout. Mirrors nixpkgs' pkgs/by-name/cl/cliproxyapi so the
# overlay is a drop-in replacement: same binary name, same --version check.
{
  lib,
  buildGoModule,
  go,
  fetchurl,
  versionCheckHook,
  # Passed from the flake so the version tracks the checked-out commit.
  src,
  rev ? "dirty",
}:

let
  # go.mod requires a newer Go than nixpkgs may ship yet; build that release
  # until nixpkgs catches up, then this falls back to nixpkgs' go.
  minGo = "1.26.10";
  goForModule =
    if lib.versionAtLeast go.version minGo then
      go
    else
      go.overrideAttrs {
        version = minGo;
        src = fetchurl {
          url = "https://go.dev/dl/go${minGo}.src.tar.gz";
          hash = "sha256:fd63e883ba2f2bfca93c1646b31ebef63e2b1e4d2b317c9789d976a68d81f36f";
        };
      };
in
(buildGoModule.override { go = goForModule; }) (finalAttrs: {
  __structuredAttrs = true;

  pname = "cliproxyapi";
  version = "8.0.29-sirati+${rev}";

  inherit src;

  vendorHash = "sha256-yMO0DdE0pjtFxWaQXV17enV8u0KA5fIUbE8Gui/RvWU=";

  subPackages = [ "cmd/server" ];

  ldflags = [
    "-s"
    "-w"
    "-X main.Version=${finalAttrs.version}"
    "-X main.Commit=${rev}"
    "-X main.BuildDate=1970-01-01"
  ];

  postInstall = ''
    mv $out/bin/server $out/bin/cliproxyapi
  '';

  nativeInstallCheckInputs = [ versionCheckHook ];
  versionCheckProgramArg = "--version";
  doInstallCheck = true;

  meta = {
    description = "sirati's fork of CLIProxyAPI";
    homepage = "https://github.com/sirati/CLIProxyAPI";
    license = lib.licenses.mit;
    mainProgram = "cliproxyapi";
  };
})
