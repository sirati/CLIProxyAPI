# Home Manager module: CLIProxyAPI as a systemd user service.
#
# With seamlessUpgrade (the default) a switch to a new build does not restart
# the service: sd-switch reloads it, ExecReload starts the new binary with
# -upgrade, and the running process hands over its listening socket, its
# state and every connection (see sdk/handoff/PROTOCOL.md). Responses that are
# streaming finish in the old process; nothing a client holds is dropped.
#
# config.yaml is not generated: it holds secrets and the server rewrites it
# (it hashes the management key, the panel saves into it). `cliproxyapi-init`
# writes one from initConfig, once; `cliproxyapi-login` adds OAuth accounts.
{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.cliproxyapi;
  exe = lib.getExe cfg.package;
  configDir = builtins.dirOf cfg.configFile;

  initTemplate = pkgs.writeText "cliproxyapi-config.yaml" (
    lib.concatStringsSep "\n" (
      [
        "config-version: 8"
        ""
        "server:"
        "  host: ${builtins.toJSON cfg.initConfig.host}"
        "  port: ${toString cfg.initConfig.port}"
        ""
        "management:"
        "  allow-remote: false"
        "  secret-key: \"@MGMT@\""
      ]
      ++ lib.optionals (cfg.managementPanel != null) [
        "  # management.html comes from nix (MANAGEMENT_STATIC_PATH), read-only."
        "  disable-auto-update-panel: true"
      ]
      ++ [
        ""
        "access:"
        "  api-keys:"
        "    - \"@CLIENT@\""
        ""
        "oauth:"
        "  auth-dir: ${builtins.toJSON cfg.initConfig.authDir}"
        ""
        cfg.initConfig.extra
      ]
    )
  );

  init = pkgs.writeShellApplication {
    name = "cliproxyapi-init";
    runtimeInputs = [
      pkgs.openssl
      pkgs.coreutils
      pkgs.gnused
    ];
    text = ''
      cfg=${lib.escapeShellArg cfg.configFile}
      mkdir -p "$(dirname "$cfg")"
      if [ -e "$cfg" ]; then
        echo "$cfg already exists; not touching it."
        exit 0
      fi
      umask 077
      mgmt=$(openssl rand -hex 32)
      client=$(openssl rand -hex 32)
      sed -e "s/@MGMT@/$mgmt/" -e "s/@CLIENT@/$client/" ${initTemplate} > "$cfg"
      echo "Wrote $cfg (mode 600)."
      echo "Client API key (what your tools send): $client"
      echo "Management key (panel login):          $mgmt"
      echo "The server hashes the management key in the file on first start,"
      echo "so note it now."
    '';
  };

  login = pkgs.writeShellApplication {
    name = "cliproxyapi-login";
    runtimeInputs = [ cfg.package ];
    text = ''
      case "''${1:-}" in
        claude) flag=-claude-login ;;
        codex) flag=-codex-login ;;
        codex-device) flag=-codex-device-login ;;
        *) echo "usage: cliproxyapi-login claude|codex|codex-device [extra flags]" >&2; exit 2 ;;
      esac
      shift
      exec cliproxyapi -config ${lib.escapeShellArg cfg.configFile} "$flag" "$@"
    '';
  };

  args = lib.escapeShellArgs ([ "-config" cfg.configFile ] ++ cfg.extraArgs);

  # ExecReload. Exit code 3 means there was nothing to take over from: the
  # running process predates handoff support (the first switch to such a
  # build) or was started without the control socket. Restart then, as
  # every switch did before.
  reload = pkgs.writeShellScript "cliproxyapi-reload" ''
    ${exe} ${args} -upgrade
    rc=$?
    if [ "$rc" -eq 3 ]; then
      exec ${pkgs.systemd}/bin/systemctl --user restart --no-block cliproxyapi.service
    fi
    exit "$rc"
  '';
in
{
  options.services.cliproxyapi = {
    enable = lib.mkEnableOption "CLIProxyAPI (sirati fork) as a user service";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.cliproxyapi;
      defaultText = lib.literalExpression "the fork's cliproxyapi package";
      description = "The CLIProxyAPI package to run.";
    };

    managementPanel = lib.mkOption {
      type = lib.types.nullOr lib.types.package;
      default = null;
      description = ''
        Directory with management.html to serve (MANAGEMENT_STATIC_PATH).
        Null lets the server download the panel itself.
      '';
    };

    configFile = lib.mkOption {
      type = lib.types.str;
      default = "${config.xdg.configHome}/cliproxyapi/config.yaml";
      defaultText = lib.literalExpression ''"''${config.xdg.configHome}/cliproxyapi/config.yaml"'';
      description = "Writable config.yaml; the unit is skipped until it exists.";
    };

    seamlessUpgrade = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = ''
        Hand the running service to a new build on switch instead of
        restarting it, so no connection or stream is interrupted.
      '';
    };

    extraArgs = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Extra command-line arguments for the server.";
    };

    environment = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      description = "Extra environment variables for the service.";
    };

    initConfig = {
      host = lib.mkOption {
        type = lib.types.str;
        default = "127.0.0.1";
        description = "server.host written by cliproxyapi-init.";
      };
      port = lib.mkOption {
        type = lib.types.port;
        default = 8317;
        description = "server.port written by cliproxyapi-init.";
      };
      authDir = lib.mkOption {
        type = lib.types.str;
        default = "~/.cli-proxy-api";
        description = "oauth.auth-dir written by cliproxyapi-init.";
      };
      extra = lib.mkOption {
        type = lib.types.lines;
        default = "";
        description = "Further top-level YAML appended by cliproxyapi-init.";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    home.packages = [
      cfg.package
      init
      login
    ];

    systemd.user.services.cliproxyapi = {
      Unit = {
        Description = "CLIProxyAPI";
        After = [ "network-online.target" ];
        ConditionPathExists = cfg.configFile;
      }
      // lib.optionalAttrs cfg.seamlessUpgrade {
        # A new build takes over instead of a restart.
        X-SwitchMethod = "reload";
      };
      Service = {
        ExecStart = "${exe} ${args}";
        WorkingDirectory = configDir;
        Environment = lib.mapAttrsToList (name: value: "${name}=${value}") (
          cfg.environment
          // lib.optionalAttrs (cfg.managementPanel != null) {
            MANAGEMENT_STATIC_PATH = "${cfg.managementPanel}";
          }
        );
        Restart = "on-failure";
        RestartSec = 5;
      }
      // lib.optionalAttrs cfg.seamlessUpgrade {
        # READY=1 once serving; the new process sends MAINPID= when it takes
        # over, which systemd accepts from any process of the unit.
        Type = "notify";
        NotifyAccess = "all";
        # The control socket ($RUNTIME_DIRECTORY/handoff.sock).
        RuntimeDirectory = "cliproxyapi";
        ExecReload = "${reload}";
        # The takeover waits for the new process to be ready.
        TimeoutSec = 180;
      };
      Install.WantedBy = [ "default.target" ];
    };
  };
}
