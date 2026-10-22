# NixOS VM test: switching to a configuration with a new CLIProxyAPI build
# hands the running user service over without interrupting any client.
#
# The workload (test/handoff/probe) keeps a 3 s Claude stream running at all
# times, sends a request every 200 ms on one keep-alive connection, and
# continues a Codex websocket conversation every 300 ms. The fake upstreams
# behind the proxy are reached through ordinary api-keys entries. Both
# switches (to the new build and back) must be reloads, move MainPID, and
# leave the workload without a single error, reconnect or upstream change.
{
  pkgs,
  self,
  home-manager,
}:
let
  system = pkgs.stdenv.hostPlatform.system;
  base = self.packages.${system}.cliproxyapi;
  # The same source as another build: a different binary and store path.
  upgraded = pkgs.callPackage ../package.nix {
    src = self;
    rev = "vmtest-upgraded";
  };
  probe = self.packages.${system}.handoff-probe;
  configYaml = pkgs.writeText "config.yaml" ''
    config-version: 8
    server:
      host: "127.0.0.1"
      port: 8317
    access:
      api-keys: ["client-key"]
    oauth:
      auth-dir: "~/.cli-proxy-api"
    api-keys:
      claude:
        - name: fake-claude
          base-url: "http://127.0.0.1:18001"
          models: [{name: "claude-fake"}]
          keys: [{api-key: "key-a"}, {api-key: "key-b"}]
      codex:
        - name: fake-codex
          base-url: "https://127.0.0.1:18002/codex"
          models: [{name: "gpt-fake"}]
          keys: [{api-key: "codex-key", websockets: true}]
  '';
in
pkgs.testers.runNixOSTest {
  name = "cliproxyapi-seamless-upgrade";

  nodes.machine =
    { lib, ... }:
    {
      imports = [ home-manager.nixosModules.home-manager ];
      virtualisation.memorySize = 2048;
      users.users.alice = {
        isNormalUser = true;
        uid = 1000;
        linger = true;
      };
      environment.systemPackages = [ probe ];
      home-manager.users.alice = {
        imports = [ self.homeManagerModules.default ];
        home.stateVersion = "25.05";
        services.cliproxyapi = {
          enable = true;
          package = base;
          extraArgs = [ "-local-model" ];
          environment.SSL_CERT_FILE = "/tmp/probe/ca.pem";
        };
      };
      specialisation.upgraded.configuration = {
        home-manager.users.alice.services.cliproxyapi.package = lib.mkForce upgraded;
      };
    };

  testScript = ''
    import json

    def as_alice(cmd):
        return f"su - alice -c 'export XDG_RUNTIME_DIR=/run/user/1000; {cmd}'"

    def user(cmd):
        return machine.succeed(as_alice(cmd))

    def wait_main_pid_changes(old):
        machine.wait_until_succeeds(as_alice(f"test \"$(systemctl --user show -p MainPID --value cliproxyapi)\" != {old}"))

    def main_pid():
        return user("systemctl --user show -p MainPID --value cliproxyapi").strip()

    def binary():
        return user("readlink /proc/$(systemctl --user show -p MainPID --value cliproxyapi)/exe").strip()

    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("user@1000.service")
    machine.wait_for_unit("home-manager-alice.service")

    machine.succeed("install -d -o alice -m 700 /tmp/probe")
    user("nohup handoff-probe upstreams -claude 127.0.0.1:18001 -codex 127.0.0.1:18002 -ca /tmp/probe/ca.pem > /tmp/probe/upstreams.log 2>&1 & echo $! > /tmp/probe/upstreams.pid")
    machine.wait_until_succeeds("test -s /tmp/probe/ca.pem")
    user("install -D -m 600 ${configYaml} ~/.config/cliproxyapi/config.yaml")
    user("systemctl --user start cliproxyapi")
    machine.wait_until_succeeds("ss -ltn | grep -q ':8317 '")
    first = main_pid()
    first_binary = binary()
    assert "${base}" in first_binary, first_binary

    user("nohup handoff-probe client -proxy http://127.0.0.1:8317 -report /tmp/probe/report.json > /tmp/probe/client.log 2>&1 & echo $! > /tmp/probe/client.pid")
    machine.sleep(5)

    with subtest("switching to the new build hands over"):
        machine.succeed("/run/current-system/specialisation/upgraded/bin/switch-to-configuration test")
        wait_main_pid_changes(first)
        second = main_pid()
        assert "${upgraded}" in binary(), binary()
        machine.sleep(6)

    with subtest("switching back hands over again"):
        machine.succeed("/run/booted-system/bin/switch-to-configuration test")
        wait_main_pid_changes(second)
        assert "${base}" in binary(), binary()
        machine.sleep(6)

    with subtest("no client noticed"):
        machine.succeed("kill $(cat /tmp/probe/client.pid)")
        machine.wait_until_succeeds("test -s /tmp/probe/report.json")
        report = json.loads(machine.succeed("cat /tmp/probe/report.json"))
        print(report)
        assert not report["stream_errors"], report["stream_errors"]
        assert not report["request_errors"], report["request_errors"]
        assert not report["turn_errors"], report["turn_errors"]
        assert report["fresh_connections"] == 0, report
        assert report["codex_conns"] == ["conn=1"], report
        assert report["streams"] >= 4 and report["requests"] >= 40 and report["turns"] >= 30, report
        stats = json.loads(machine.succeed("curl -s http://127.0.0.1:18001/_probe/stats"))
        assert stats["codex_connections"] == 1 and not stats["codex_errors"], stats

    with subtest("the unit was reloaded, never restarted"):
        journal = user("journalctl --user -u cliproxyapi --no-pager")
        assert journal.count("took over") == 2, journal
        assert "Reloaded" in journal or "reloaded" in journal.lower(), journal
        starts = journal.count("Started CLIProxyAPI")
        assert starts == 1, f"unit started {starts} times"
        user("systemctl --user is-active cliproxyapi")
        # Every replaced process is gone once its connections and relays are.
        machine.wait_until_succeeds("test $(pgrep -u alice -fc 'bin/cliproxyapi') -eq 1")

    with subtest("a service without the control socket is restarted instead"):
        # What the first switch from a pre-handoff build looks like.
        before = main_pid()
        user("rm /run/user/1000/cliproxyapi/handoff.sock")
        user("systemctl --user reload cliproxyapi")
        wait_main_pid_changes(before)
        user("systemctl --user is-active cliproxyapi")
        machine.wait_until_succeeds(as_alice("test -S /run/user/1000/cliproxyapi/handoff.sock"))
        journal = user("journalctl --user -u cliproxyapi --no-pager")
        assert journal.count("Started CLIProxyAPI") == 2, journal

    machine.succeed("kill $(cat /tmp/probe/upstreams.pid)")
  '';
}
