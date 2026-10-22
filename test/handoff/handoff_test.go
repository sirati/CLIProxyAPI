// Package handoff_test runs two real proxy processes and hands the service
// from one to the other while clients are busy.
package handoff_test

import (
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/test/handoff/fakeupstream"
	"github.com/router-for-me/CLIProxyAPI/v8/test/handoff/workload"
)

var proxyBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "cliproxy-handoff-bin")
	if err != nil {
		panic(err)
	}
	proxyBinary = filepath.Join(dir, "cliproxyapi")
	build := exec.Command("go", "build", "-tags", "handofftest", "-o", proxyBinary, "./cmd/server")
	build.Dir = filepath.Join("..", "..")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		panic("building the proxy: " + err.Error())
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type proxyEnv struct {
	t       *testing.T
	dir     string
	config  string
	socket  string
	port    int
	env     []string
	logs    []string
	started []int
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func newProxyEnv(t *testing.T, f *fakes) *proxyEnv {
	t.Helper()
	dir := t.TempDir()
	env := &proxyEnv{t: t, dir: dir, config: filepath.Join(dir, "config.yaml"), socket: filepath.Join(dir, "handoff.sock"), port: freePort(t)}
	ca := filepath.Join(dir, "ca.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.codexServer.Certificate().Raw})
	if err := os.WriteFile(ca, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`config-version: 8
server:
  host: "127.0.0.1"
  port: %d
access:
  api-keys:
    - "client-key"
oauth:
  auth-dir: %q
routing:
  strategy: "fill-first"
api-keys:
  claude:
    - name: fake-claude
      base-url: %q
      models:
        - name: "claude-fake"
      keys:
        - api-key: "key-a"
        - api-key: "key-b"
  codex:
    - name: fake-codex
      base-url: %q
      models:
        - name: "gpt-fake"
      keys:
        - api-key: "codex-key"
          websockets: true
`, env.port, filepath.Join(dir, "auth"), f.claudeServer.URL, f.codexServer.URL+"/codex")
	if err := os.WriteFile(env.config, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	env.env = append(os.Environ(), "SSL_CERT_FILE="+ca, "CLIPROXY_HANDOFF_SOCKET="+env.socket)
	t.Cleanup(env.stopAll)
	return env
}

func (e *proxyEnv) logFile(name string) *os.File {
	path := filepath.Join(e.dir, name+".log")
	file, err := os.Create(path)
	if err != nil {
		e.t.Fatal(err)
	}
	e.logs = append(e.logs, path)
	return file
}

// start runs the first service and waits until it serves.
func (e *proxyEnv) start() int {
	e.t.Helper()
	log := e.logFile("service-0")
	cmd := exec.Command(proxyBinary, "-config", e.config, "-local-model")
	cmd.Env, cmd.Stdout, cmd.Stderr = e.env, log, log
	if err := cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	go func() { _ = cmd.Wait() }()
	e.started = append(e.started, cmd.Process.Pid)
	e.waitServing()
	return cmd.Process.Pid
}

var tookOver = regexp.MustCompile(`upgrade: pid (\d+) took over`)

// upgrade runs `-upgrade` like systemd's ExecReload and returns the new pid.
func (e *proxyEnv) upgrade() int {
	e.t.Helper()
	pid, err := e.tryUpgrade()
	if err != nil {
		e.dumpLogs()
		e.t.Fatalf("upgrade failed: %v", err)
	}
	return pid
}

// tryUpgrade runs `-upgrade` with extra environment.
func (e *proxyEnv) tryUpgrade(extraEnv ...string) (int, error) {
	// The new service inherits the command's output, as under systemd, so
	// the output goes to a file and only the command's exit is awaited.
	log := e.logFile(fmt.Sprintf("upgrade-%d", len(e.logs)))
	cmd := exec.Command(proxyBinary, "-config", e.config, "-local-model", "-upgrade")
	cmd.Env, cmd.Stdout, cmd.Stderr = append(append([]string(nil), e.env...), extraEnv...), log, log
	if err := cmd.Run(); err != nil {
		return 0, err
	}
	out, _ := os.ReadFile(log.Name())
	match := tookOver.FindSubmatch(out)
	if match == nil {
		return 0, fmt.Errorf("upgrade printed %q", out)
	}
	pid, _ := strconv.Atoi(string(match[1]))
	e.started = append(e.started, pid)
	return pid, nil
}

func (e *proxyEnv) waitServing() {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(e.socket); err == nil {
			if conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", e.port)); err == nil {
				_ = conn.Close()
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.dumpLogs()
	e.t.Fatal("service did not start")
}

func alive(pid int) bool {
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	fields := strings.Fields(string(status))
	return len(fields) > 2 && fields[2] != "Z"
}

func (e *proxyEnv) waitExit(pid int, within time.Duration) {
	e.t.Helper()
	deadline := time.Now().Add(within)
	for alive(pid) {
		if time.Now().After(deadline) {
			e.dumpLogs()
			e.t.Fatalf("pid %d still running", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (e *proxyEnv) stopAll() {
	for _, pid := range e.started {
		if alive(pid) {
			_ = syscall.Kill(pid, syscall.SIGTERM)
		}
	}
	for _, pid := range e.started {
		deadline := time.Now().Add(10 * time.Second)
		for alive(pid) && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
		if alive(pid) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	// A race-enabled build (GOFLAGS=-race) reports races on stderr.
	for _, path := range e.logs {
		if data, _ := os.ReadFile(path); strings.Contains(string(data), "WARNING: DATA RACE") {
			e.t.Errorf("%s reports a data race", filepath.Base(path))
		}
	}
	if e.t.Failed() {
		e.dumpLogs()
	}
}

func (e *proxyEnv) dumpLogs() {
	for _, path := range e.logs {
		data, _ := os.ReadFile(path)
		e.t.Logf("==== %s ====\n%s", filepath.Base(path), data)
	}
}

func (e *proxyEnv) url(path string) string { return fmt.Sprintf("http://127.0.0.1:%d%s", e.port, path) }

const clientKey = "client-key"

func claudeStream(client *http.Client, url, text string) (string, bool, error) {
	return workload.ClaudeStream(client, url, clientKey, text)
}

func dialCodex(t *testing.T, e *proxyEnv) *workload.CodexSession {
	t.Helper()
	session, err := workload.DialCodex(strings.Replace(e.url("/v1/responses"), "http", "ws", 1), clientKey)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func turn(t *testing.T, session *workload.CodexSession, text string) string {
	t.Helper()
	got, err := session.Turn(text)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// fakes are the upstreams, served the way the proxy reaches real ones.
type fakes struct {
	claude       *fakeupstream.Claude
	codex        *fakeupstream.Codex
	claudeServer *httptest.Server
	codexServer  *httptest.Server
}

func newFakes(t *testing.T) *fakes {
	f := &fakes{claude: &fakeupstream.Claude{}, codex: &fakeupstream.Codex{}}
	f.claudeServer = httptest.NewServer(f.claude)
	f.codexServer = httptest.NewTLSServer(f.codex)
	t.Cleanup(func() {
		f.claudeServer.Close()
		f.codexServer.Close()
	})
	return f
}

func TestUpgradeKeepsEveryConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("runs real proxy processes")
	}
	f := newFakes(t)
	env := newProxyEnv(t, f)
	first := env.start()
	messages := env.url("/v1/messages")

	// A keep-alive connection that will be idle at the handoff.
	keepAlive := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
	if body, _, err := claudeStream(keepAlive, messages, "hello"); err != nil || !strings.Contains(body, "[2|") {
		t.Fatalf("first request: %v %q", err, body)
	}
	// A Codex session with one turn done.
	codexSession := dialCodex(t, env)
	if got := turn(t, codexSession, "one"); got != "conn=1 turn=1" {
		t.Fatalf("turn 1: %q", got)
	}

	// A long stream and a request whose first credential fails after the cut.
	type result struct {
		body string
		err  error
	}
	streamDone := make(chan result, 1)
	go func() {
		body, _, err := claudeStream(&http.Client{}, messages, "stream:30")
		streamDone <- result{body, err}
	}()
	rerunDone := make(chan result, 1)
	go func() {
		body, _, err := claudeStream(&http.Client{}, messages, "fail-first")
		rerunDone <- result{body, err}
	}()
	time.Sleep(500 * time.Millisecond)

	second := env.upgrade()
	if second == first {
		t.Fatal("upgrade reported the old pid")
	}

	stream := <-streamDone
	if stream.err != nil || !strings.Contains(stream.body, "[29|") || !strings.Contains(stream.body, "message_stop") {
		t.Fatalf("the stream running during the upgrade broke: %v\n%s", stream.err, stream.body)
	}
	rerun := <-rerunDone
	failed := f.claude.FailedKey()
	if rerun.err != nil || failed == "" || strings.Contains(rerun.body, "|"+failed+"]") || !strings.Contains(rerun.body, "message_stop") {
		t.Fatalf("the request that needed a decision after the cut: %v\n%s", rerun.err, rerun.body)
	}
	body, reused, err := claudeStream(keepAlive, messages, "again")
	if err != nil || !reused || !strings.Contains(body, "message_stop") {
		t.Fatalf("keep-alive connection after the upgrade: reused=%v err=%v %q", reused, err, body)
	}
	if got := turn(t, codexSession, "two"); got != "conn=1 turn=2" {
		t.Fatalf("turn 2 after the upgrade: %q (want the same upstream connection)", got)
	}
	if !alive(first) {
		t.Fatal("the first process exited while it still relays the upstream TLS session")
	}

	// A second upgrade: the middle process passes the stream on and leaves.
	third := env.upgrade()
	if got := turn(t, codexSession, "three"); got != "conn=1 turn=3" {
		t.Fatalf("turn 3 after the second upgrade: %q", got)
	}
	env.waitExit(second, 20*time.Second)
	if !alive(first) || !alive(third) {
		t.Fatalf("first alive=%v third alive=%v", alive(first), alive(third))
	}

	// Closing the session ends the relay, and with it the first process.
	_ = codexSession.Close()
	env.waitExit(first, 20*time.Second)
	if errs := f.codex.ProtocolErrors(); len(errs) > 0 {
		t.Fatalf("upstream saw broken continuity: %v", errs)
	}
	if n := f.codex.Connections(); n != 1 {
		t.Fatalf("upstream saw %d websocket connections, want 1", n)
	}

	// The last service still serves new connections.
	if body, _, err := claudeStream(&http.Client{}, messages, "after"); err != nil || !strings.Contains(body, "message_stop") {
		t.Fatalf("new connection to the final service: %v %q", err, body)
	}
}

func TestFailedTakeoverLeavesTheServiceRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("runs real proxy processes")
	}
	f := newFakes(t)
	env := newProxyEnv(t, f)
	first := env.start()
	messages := env.url("/v1/messages")
	keepAlive := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
	if _, _, err := claudeStream(keepAlive, messages, "hello"); err != nil {
		t.Fatal(err)
	}

	// The new process dies after the old one froze and handed its listener.
	if _, err := env.tryUpgrade("CLIPROXY_HANDOFF_FAULT=before-ready"); err == nil {
		t.Fatal("upgrade reported success although the new process failed")
	}
	if !alive(first) {
		t.Fatal("the running service exited")
	}
	// It took everything back: the listener, its decisions, its connections.
	body, reused, err := claudeStream(keepAlive, messages, "still here")
	if err != nil || !reused || !strings.Contains(body, "message_stop") {
		t.Fatalf("keep-alive connection after a failed upgrade: reused=%v err=%v %q", reused, err, body)
	}
	if body, _, err := claudeStream(&http.Client{}, messages, "new connection"); err != nil || !strings.Contains(body, "message_stop") {
		t.Fatalf("new connection after a failed upgrade: %v %q", err, body)
	}

	// And a later upgrade still works.
	second := env.upgrade()
	if body, reused, err := claudeStream(keepAlive, messages, "moved"); err != nil || !reused || !strings.Contains(body, "message_stop") {
		t.Fatalf("keep-alive connection after the real upgrade: reused=%v err=%v", reused, err)
	}
	env.waitExit(first, 20*time.Second)
	if !alive(second) {
		t.Fatal("the new service is not running")
	}
}
