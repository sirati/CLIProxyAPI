package cliproxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/handoff"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/redisqueue"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkhandoff "github.com/router-for-me/CLIProxyAPI/v8/sdk/handoff"
	log "github.com/sirupsen/logrus"
)

// Seamless binary upgrades. A running service listens on a control socket.
// A new binary started with Takeover connects to it; the old service freezes
// (it makes no decision and writes nothing from then on), hands over its
// listening socket and state, and once the new service is serving it moves
// every connection over at its next idle point. Responses already streaming
// finish in the old process, which exits when it holds nothing but relays for
// upstream TLS sessions, and those end with their sockets.

func init() {
	handoff.AddCapability(sdkhandoff.KindControlListener)
}

const (
	handoffReadyTimeout = 2 * time.Minute
	handoffSweepEvery   = 50 * time.Millisecond
	handoffRefreshEvery = 15 * time.Minute
)

// HandoffConfig enables seamless upgrades for a service.
type HandoffConfig struct {
	// SocketPath is the control socket; empty disables handoff support.
	SocketPath string
	// Takeover makes the service take over from the one at SocketPath.
	Takeover bool
	// Binary identifies this build in the hello.
	Binary string
	// Status receives "ok" or an error once a takeover finished; it is the
	// pipe the -upgrade command waits on.
	Status *os.File
}

type serviceHandoff struct {
	cfg HandoffConfig

	// New side: what the old process sent.
	ctrl            *sdkhandoff.Conn
	listener        net.Listener
	controlListener *net.UnixListener
	affinity        []coreauth.HandoffAffinityGroup
	cooldown        []coreauth.CooldownStateRecord
	usageQueue      []redisqueue.HandoffQueueItem
	earlyEvents     []sdkhandoff.Message

	// Old side.
	takeoverMu sync.Mutex
}

// EnableHandoff turns on seamless upgrades; call it before Run.
func (s *Service) EnableHandoff(cfg HandoffConfig) {
	if s == nil {
		return
	}
	s.handoff.cfg = cfg
	handoff.SetEnabled(cfg.SocketPath != "")
}

func handoffHello(binary string) sdkhandoff.Hello {
	return sdkhandoff.Hello{
		Protocols:    []int{sdkhandoff.ProtocolVersion},
		Binary:       binary,
		PID:          os.Getpid(),
		Capabilities: handoff.Capabilities(),
	}
}

func supportsProtocol(hello sdkhandoff.Hello) bool {
	for _, version := range hello.Protocols {
		if version == sdkhandoff.ProtocolVersion {
			return true
		}
	}
	return false
}

// beginHandoffTakeover runs before the new service loads anything: the old
// service freezes and sends its listener and state.
func (s *Service) beginHandoffTakeover() error {
	h := &s.handoff
	if !h.cfg.Takeover {
		return nil
	}
	ctrl, errDial := sdkhandoff.Dial(h.cfg.SocketPath)
	if errDial != nil {
		return fmt.Errorf("handoff: connect to the running service: %w", errDial)
	}
	h.ctrl = ctrl
	if errSend := ctrl.SendJSON(sdkhandoff.TypeHello, "", "", handoffHello(h.cfg.Binary)); errSend != nil {
		return errSend
	}
	msg, _, errRecv := ctrl.Recv()
	if errRecv != nil {
		return fmt.Errorf("handoff: hello: %w", errRecv)
	}
	var peer sdkhandoff.Hello
	if msg.Type != sdkhandoff.TypeHello || msg.Decode(&peer) != nil || !supportsProtocol(peer) {
		return errors.New("handoff: the running service speaks no common protocol")
	}
	log.Infof("handoff: taking over from %s (pid %d)", peer.Binary, peer.PID)
	if errSend := ctrl.Send(sdkhandoff.Message{Type: sdkhandoff.TypeTakeover}); errSend != nil {
		return errSend
	}
	for {
		msg, files, errRecv := ctrl.Recv()
		if errRecv != nil {
			return fmt.Errorf("handoff: receiving state: %w", errRecv)
		}
		switch msg.Type {
		case sdkhandoff.TypeStateEnd:
			return nil
		case sdkhandoff.TypeEvent:
			h.earlyEvents = append(h.earlyEvents, msg)
			continue
		case sdkhandoff.TypeItem:
		default:
			closeFiles(files)
			return fmt.Errorf("handoff: unexpected %s before the state ended", msg.Type)
		}
		if errItem := h.receiveStateItem(msg, files); errItem != nil {
			return errItem
		}
	}
}

func (h *serviceHandoff) receiveStateItem(msg sdkhandoff.Message, files []*os.File) error {
	switch msg.Kind {
	case sdkhandoff.KindListenerTCP, sdkhandoff.KindControlListener:
		if len(files) != 1 {
			closeFiles(files)
			return fmt.Errorf("handoff: %s without its socket", msg.Kind)
		}
		listener, errListen := net.FileListener(files[0])
		_ = files[0].Close()
		if errListen != nil {
			return errListen
		}
		if msg.Kind == sdkhandoff.KindListenerTCP {
			h.listener = listener
			return nil
		}
		unixListener, ok := listener.(*net.UnixListener)
		if !ok {
			_ = listener.Close()
			return errors.New("handoff: control listener is not a unix socket")
		}
		h.controlListener = unixListener
		return nil
	case sdkhandoff.KindStateAffinity:
		closeFiles(files)
		return msg.Decode(&h.affinity)
	case sdkhandoff.KindStateCooldown:
		closeFiles(files)
		return msg.Decode(&h.cooldown)
	case sdkhandoff.KindStateUsageQueue:
		closeFiles(files)
		return msg.Decode(&h.usageQueue)
	default:
		closeFiles(files)
		log.Warnf("handoff: ignoring state item %s", msg.Kind)
		return nil
	}
}

// importHandoffAuthState applies the old service's runtime auth state once
// the auth files are loaded.
func (s *Service) importHandoffAuthState(ctx context.Context) {
	h := &s.handoff
	if h.ctrl == nil || s.coreManager == nil {
		return
	}
	if errCooldown := s.coreManager.ImportHandoffCooldown(ctx, h.cooldown); errCooldown != nil {
		log.Warnf("handoff: cooldown state not restored: %v", errCooldown)
	}
	s.coreManager.ImportHandoffAffinity(h.affinity)
	h.cooldown, h.affinity = nil, nil
}

// attachHandoffServer gives the new API server the inherited listener.
func (s *Service) attachHandoffServer() {
	h := &s.handoff
	if s.server == nil {
		return
	}
	if h.listener != nil {
		s.server.SetInheritedListener(h.listener)
		h.listener = nil
	}
	if len(h.usageQueue) > 0 {
		redisqueue.ImportHandoffQueue(h.usageQueue)
		h.usageQueue = nil
	}
}

// startHandoff runs once the API server is serving.
func (s *Service) startHandoff(ctx context.Context) {
	h := &s.handoff
	if h.cfg.SocketPath == "" {
		_ = handoff.Notify("READY=1")
		return
	}
	if h.ctrl != nil {
		for _, event := range h.earlyEvents {
			s.applyHandoffEvent(ctx, event)
		}
		h.earlyEvents = nil
		if errReady := h.ctrl.Send(sdkhandoff.Message{Type: sdkhandoff.TypeReady}); errReady != nil {
			s.reportTakeover(fmt.Errorf("handoff: ready: %w", errReady))
			return
		}
		// systemd follows the new process from here on.
		if errNotify := handoff.Notify(fmt.Sprintf("MAINPID=%d\nREADY=1", os.Getpid())); errNotify != nil {
			log.Warnf("handoff: notify systemd: %v", errNotify)
		}
		log.Info("handoff: serving; the previous process is draining")
		s.reportTakeover(nil)
		go s.adoptFromOldProcess(ctx, h.ctrl)
	} else {
		_ = handoff.Notify("READY=1")
	}
	listener := h.controlListener
	if listener == nil {
		var errListen error
		if listener, errListen = listenControlSocket(h.cfg.SocketPath); errListen != nil {
			log.Errorf("handoff: control socket %s: %v; seamless upgrades are off", h.cfg.SocketPath, errListen)
			return
		}
	}
	go s.acceptTakeovers(ctx, listener)
}

func (s *Service) reportTakeover(err error) {
	status := s.handoff.cfg.Status
	if status == nil {
		return
	}
	text := "ok"
	if err != nil {
		text = err.Error()
	}
	_, _ = status.WriteString(text)
	_ = status.Close()
	s.handoff.cfg.Status = nil
}

func listenControlSocket(path string) (*net.UnixListener, error) {
	if conn, errDial := net.Dial("unix", path); errDial == nil {
		_ = conn.Close()
		return nil, errors.New("another service is listening")
	}
	_ = os.Remove(path)
	return net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
}

// adoptFromOldProcess takes connections and events from the old process
// until it has nothing left.
func (s *Service) adoptFromOldProcess(ctx context.Context, ctrl *sdkhandoff.Conn) {
	defer func() { _ = ctrl.Close() }()
	for {
		msg, files, errRecv := ctrl.Recv()
		if errRecv != nil {
			log.Infof("handoff: previous process disconnected: %v", errRecv)
			return
		}
		switch msg.Type {
		case sdkhandoff.TypeItem:
			if errAdopt := s.server.AdoptHandoffItem(ctx, msg, files); errAdopt != nil {
				log.Errorf("handoff: adopting %s: %v", msg.Kind, errAdopt)
			}
		case sdkhandoff.TypeEvent:
			closeFiles(files)
			s.applyHandoffEvent(ctx, msg)
		case sdkhandoff.TypeDone:
			closeFiles(files)
			log.Info("handoff: previous process handed over everything")
			return
		default:
			closeFiles(files)
		}
	}
}

func (s *Service) applyHandoffEvent(ctx context.Context, msg sdkhandoff.Message) {
	switch msg.Kind {
	case sdkhandoff.KindEventResult:
		if s.coreManager != nil {
			if errApply := s.coreManager.ApplyHandoffResult(ctx, msg.Data); errApply != nil {
				log.Warnf("handoff: forwarded result: %v", errApply)
			}
		}
	case sdkhandoff.KindEventUsage:
		var event redisqueue.HandoffUsageEvent
		if errDecode := json.Unmarshal(msg.Data, &event); errDecode == nil {
			redisqueue.ApplyHandoffEvent(event)
		}
	}
}

// acceptTakeovers serves the control socket of a running service.
func (s *Service) acceptTakeovers(ctx context.Context, listener *net.UnixListener) {
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		conn, errAccept := listener.AcceptUnix()
		if errAccept != nil {
			return
		}
		if s.serveTakeover(ctx, sdkhandoff.NewConn(conn), listener) {
			return
		}
	}
}

// handoffSink sends to the new process over the control connection.
type handoffSink struct {
	conn *sdkhandoff.Conn
	peer sdkhandoff.Hello
}

func (k *handoffSink) Send(kind, id string, payload any, files ...*os.File) error {
	defer closeFiles(files)
	if !k.peer.Supports(kind) {
		return fmt.Errorf("handoff: the new process does not take %s", kind)
	}
	return k.conn.SendJSON(sdkhandoff.TypeItem, kind, id, payload, files...)
}

func (k *handoffSink) Event(kind string, payload any) {
	if !k.peer.Supports(kind) {
		return
	}
	if errSend := k.conn.SendJSON(sdkhandoff.TypeEvent, kind, "", payload); errSend != nil {
		log.Warnf("handoff: forwarding %s: %v", kind, errSend)
	}
}

func (k *handoffSink) Supports(kind string) bool { return k.peer.Supports(kind) }

// serveTakeover hands this service to the process on conn. It reports
// whether the service handed off (and must stop accepting takeovers).
func (s *Service) serveTakeover(ctx context.Context, conn *sdkhandoff.Conn, controlListener *net.UnixListener) bool {
	s.handoff.takeoverMu.Lock()
	defer s.handoff.takeoverMu.Unlock()
	fail := func(err error) bool {
		log.Errorf("handoff: takeover refused: %v", err)
		_ = conn.Send(sdkhandoff.Message{Type: sdkhandoff.TypeError, Error: err.Error()})
		_ = conn.Close()
		return false
	}
	msg, files, errRecv := conn.Recv()
	closeFiles(files)
	var peer sdkhandoff.Hello
	if errRecv != nil || msg.Type != sdkhandoff.TypeHello || msg.Decode(&peer) != nil {
		return fail(errors.New("expected a hello"))
	}
	if errSend := conn.SendJSON(sdkhandoff.TypeHello, "", "", handoffHello(s.handoff.cfg.Binary)); errSend != nil {
		return fail(errSend)
	}
	if !supportsProtocol(peer) || !peer.Supports(sdkhandoff.KindListenerTCP) {
		return fail(errors.New("the new process cannot take the listener"))
	}
	msg, files, errRecv = conn.Recv()
	closeFiles(files)
	if errRecv != nil || msg.Type != sdkhandoff.TypeTakeover {
		return fail(errors.New("expected a takeover request"))
	}
	log.Infof("handoff: %s (pid %d) takes over", peer.Binary, peer.PID)

	// From here this process decides nothing and writes nothing.
	sink := &handoffSink{conn: conn, peer: peer}
	handoff.Freeze(sink)
	thaw := s.coreManager.FreezeForHandoff()
	rollback := func(err error, listenerFile *os.File) bool {
		handoff.Unfreeze()
		if listenerFile != nil {
			if errRestore := s.server.RestoreListener(listenerFile); errRestore != nil {
				log.Errorf("handoff: taking the listener back failed: %v", errRestore)
			}
		}
		thaw(ctx, handoffRefreshEvery)
		log.Errorf("handoff: the new process failed, this one carries on: %v", err)
		_ = conn.Close()
		return false
	}

	listenerFile, errRelease := s.server.ReleaseListener()
	if errRelease != nil {
		return rollback(errRelease, nil)
	}
	keep, errDup := dupFile(listenerFile)
	if errDup != nil {
		return rollback(errDup, listenerFile)
	}
	if errSend := sink.Send(sdkhandoff.KindListenerTCP, "", nil, listenerFile); errSend != nil {
		return rollback(errSend, keep)
	}
	if peer.Supports(sdkhandoff.KindControlListener) {
		if controlFile, errFile := controlListener.File(); errFile == nil {
			_ = sink.Send(sdkhandoff.KindControlListener, "", nil, controlFile)
		}
	}
	if s.coreManager != nil {
		_ = sink.Send(sdkhandoff.KindStateCooldown, "", s.coreManager.ExportHandoffCooldown())
		_ = sink.Send(sdkhandoff.KindStateAffinity, "", s.coreManager.ExportHandoffAffinity())
	}
	_ = sink.Send(sdkhandoff.KindStateUsageQueue, "", redisqueue.ExportHandoffQueue())
	if errEnd := conn.Send(sdkhandoff.Message{Type: sdkhandoff.TypeStateEnd}); errEnd != nil {
		return rollback(errEnd, keep)
	}

	ready := make(chan error, 1)
	go func() {
		msg, files, errRecv := conn.Recv()
		closeFiles(files)
		if errRecv == nil && msg.Type != sdkhandoff.TypeReady {
			errRecv = fmt.Errorf("expected ready, got %s", msg.Type)
		}
		ready <- errRecv
	}()
	select {
	case errReady := <-ready:
		if errReady != nil {
			return rollback(errReady, keep)
		}
	case <-time.After(handoffReadyTimeout):
		return rollback(errors.New("the new process did not become ready"), keep)
	}
	_ = keep.Close()
	// The new process listens on the same socket now: close ours without
	// removing the path.
	controlListener.SetUnlinkOnClose(false)
	_ = controlListener.Close()
	log.Info("handoff: the new process is serving; this one only finishes what is running")
	s.stopBackgroundForHandoff()
	go s.drainAfterHandoff(conn)
	return true
}

// stopBackgroundForHandoff stops what only matters to a live service.
func (s *Service) stopBackgroundForHandoff() {
	s.cfgMu.RLock()
	watcherCancel := s.watcherCancel
	s.cfgMu.RUnlock()
	if watcherCancel != nil {
		watcherCancel()
	}
	if watcher := s.currentWatcher(); watcher != nil {
		_ = watcher.Stop()
	}
	if s.authQueueStop != nil {
		s.authQueueStop()
		s.authQueueStop = nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.shutdownPprof(shutdownCtx)
	_ = s.shutdownDiscovery()
}

// drainAfterHandoff moves every connection over as it goes idle, then waits
// for the relays and ends this process.
func (s *Service) drainAfterHandoff(conn *sdkhandoff.Conn) {
	for {
		remaining := s.server.HandoffIdleConns() + handoff.HandoffSessions()
		if remaining == 0 {
			break
		}
		time.Sleep(handoffSweepEvery)
	}
	_ = conn.Send(sdkhandoff.Message{Type: sdkhandoff.TypeDone})
	_ = conn.Close()
	log.Info("handoff: every connection moved; waiting for upstream relays to end")
	handoff.WaitRelays()
	log.Info("handoff: done, exiting")
	s.homeMu.Lock()
	cancel := s.runCancel
	s.homeMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func dupFile(file *os.File) (*os.File, error) {
	fd, errDup := syscall.Dup(int(file.Fd()))
	if errDup != nil {
		return nil, errDup
	}
	syscall.CloseOnExec(fd)
	return os.NewFile(uintptr(fd), file.Name()), nil
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		_ = file.Close()
	}
}
