package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/cmd"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy"
)

// Seamless upgrades: a service started with a control socket can be handed to
// a newer binary with `cliproxyapi -config ... -upgrade`, which is what the
// systemd unit runs as ExecReload. -upgrade starts this binary again with
// -takeover and reports whether it took over.

const (
	handoffStatusFDEnv = "CLIPROXY_HANDOFF_STATUS_FD"
	upgradeTimeout     = 3 * time.Minute
	// exitNoService tells the caller nothing could be taken over (the running
	// service predates handoff support or is not running); the systemd unit
	// falls back to a restart then.
	exitNoService = 3
)

var errNoService = errors.New("no service that supports handoff is running")

var (
	handoffSocket string
	upgradeFlag   bool
	takeoverFlag  bool
)

func registerHandoffFlags() {
	flag.StringVar(&handoffSocket, "handoff-socket", defaultHandoffSocket(), "Control socket for seamless upgrades (default: $CLIPROXY_HANDOFF_SOCKET, else handoff.sock in $RUNTIME_DIRECTORY; empty disables)")
	flag.BoolVar(&upgradeFlag, "upgrade", false, "Hand the running service over to this binary without dropping connections")
	flag.BoolVar(&takeoverFlag, "takeover", false, "Take over from the running service (started by -upgrade)")
}

func defaultHandoffSocket() string {
	if path := strings.TrimSpace(os.Getenv("CLIPROXY_HANDOFF_SOCKET")); path != "" {
		return path
	}
	if dirs := strings.TrimSpace(os.Getenv("RUNTIME_DIRECTORY")); dirs != "" {
		dir, _, _ := strings.Cut(dirs, ":")
		return filepath.Join(dir, "handoff.sock")
	}
	return ""
}

// applyHandoffFlags runs right after flag.Parse.
func applyHandoffFlags() {
	if upgradeFlag {
		errUpgrade := runUpgrade()
		if errors.Is(errUpgrade, errNoService) {
			fmt.Fprintln(os.Stderr, "upgrade:", errUpgrade)
			os.Exit(exitNoService)
		}
		if errUpgrade != nil {
			fmt.Fprintln(os.Stderr, "upgrade failed:", errUpgrade)
			os.Exit(1)
		}
		os.Exit(0)
	}
	config := cliproxy.HandoffConfig{
		SocketPath: handoffSocket,
		Takeover:   takeoverFlag,
		Binary:     "cliproxyapi " + Version,
	}
	if takeoverFlag {
		if config.SocketPath == "" {
			fmt.Fprintln(os.Stderr, "-takeover needs -handoff-socket")
			os.Exit(1)
		}
		if raw := os.Getenv(handoffStatusFDEnv); raw != "" {
			if fd, errParse := strconv.Atoi(raw); errParse == nil {
				config.Status = os.NewFile(uintptr(fd), "handoff-status")
			}
			_ = os.Unsetenv(handoffStatusFDEnv)
		}
	}
	cmd.SetHandoffConfig(config)
}

// runUpgrade starts this binary in takeover mode and waits until it reports
// that it serves, or why it could not. It does not wait for the new service
// to exit: that is the service now.
func runUpgrade() error {
	if handoffSocket == "" {
		return errors.New("no handoff socket: set -handoff-socket or run under systemd with RuntimeDirectory")
	}
	probe, errDial := net.Dial("unix", handoffSocket)
	if errDial != nil {
		return fmt.Errorf("%w at %s: %v", errNoService, handoffSocket, errDial)
	}
	_ = probe.Close()
	exe, errExe := os.Executable()
	if errExe != nil {
		return errExe
	}
	args := []string{"-takeover", "-handoff-socket", handoffSocket}
	for _, arg := range os.Args[1:] {
		if arg == "-upgrade" || arg == "--upgrade" || arg == "-upgrade=true" || arg == "--upgrade=true" {
			continue
		}
		args = append(args, arg)
	}
	statusRead, statusWrite, errPipe := os.Pipe()
	if errPipe != nil {
		return errPipe
	}
	daemon := exec.Command(exe, args...)
	daemon.Stdout, daemon.Stderr = os.Stdout, os.Stderr
	daemon.ExtraFiles = []*os.File{statusWrite}
	daemon.Env = append(os.Environ(), handoffStatusFDEnv+"=3")
	if errStart := daemon.Start(); errStart != nil {
		_ = statusRead.Close()
		_ = statusWrite.Close()
		return errStart
	}
	_ = statusWrite.Close()

	status := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(statusRead)
		status <- strings.TrimSpace(string(data))
	}()
	select {
	case text := <-status:
		switch text {
		case "ok":
			fmt.Printf("upgrade: pid %d took over\n", daemon.Process.Pid)
			_ = daemon.Process.Release()
			return nil
		case "":
			return errors.New("the new process exited before taking over")
		default:
			return errors.New(text)
		}
	case <-time.After(upgradeTimeout):
		_ = daemon.Process.Kill()
		return errors.New("the new process did not take over in time")
	}
}
