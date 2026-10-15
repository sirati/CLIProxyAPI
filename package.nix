# Builds this checkout. Mirrors nixpkgs' pkgs/by-name/cl/cliproxyapi so the
# overlay is a drop-in replacement: same binary name, same --version check.
{
  lib,
  buildGoModule,
  versionCheckHook,
  # Passed from the flake so the version tracks the checked-out commit.
  src,
  rev ? "dirty",
}:

buildGoModule (finalAttrs: {
  __structuredAttrs = true;

  pname = "cliproxyapi";
  version = "8.0.29-sirati+${rev}";

  inherit src;

  vendorHash = "sha256-r3yWkdMcM40G9jV7MxW/qNv3E9WrHavFilW24quEf+8=";

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
