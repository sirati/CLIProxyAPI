package api

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/handoff"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/openai"
	sdkhandoff "github.com/router-for-me/CLIProxyAPI/v8/sdk/handoff"
	log "github.com/sirupsen/logrus"
)

// Binary handoff support for the API server: the listening socket and every
// plain HTTP/1.1 connection can move to a new process. Upstream code only
// calls listen, wrapHandoffConn, configureHandoff and handoffRerunMiddleware.

func init() {
	handoff.AddCapability(sdkhandoff.KindListenerTCP)
	handoff.AddCapability(sdkhandoff.KindConnHTTP1)
}

type serverHandoff struct {
	mu        sync.Mutex
	inherited net.Listener
	listener  *handoffListener
	conns     map[*handoff.Conn]struct{}
}

// SetInheritedListener makes Start serve l instead of opening the port.
func (s *Server) SetInheritedListener(l net.Listener) {
	s.handoff.mu.Lock()
	s.handoff.inherited = l
	s.handoff.mu.Unlock()
}

func (s *Server) listen(addr string) (net.Listener, error) {
	s.handoff.mu.Lock()
	inherited := s.handoff.inherited
	s.handoff.inherited = nil
	s.handoff.mu.Unlock()
	listener := inherited
	if listener == nil {
		var errListen error
		if listener, errListen = net.Listen("tcp", addr); errListen != nil {
			return nil, errListen
		}
	}
	if !handoff.Enabled() {
		return listener, nil
	}
	wrapped := newHandoffListener(listener)
	s.handoff.mu.Lock()
	s.handoff.listener = wrapped
	s.handoff.mu.Unlock()
	return wrapped, nil
}

// ReleaseListener gives the listening socket away. Accept keeps blocking
// afterwards, so the server stays up for the connections it still holds.
func (s *Server) ReleaseListener() (*os.File, error) {
	s.handoff.mu.Lock()
	listener := s.handoff.listener
	s.handoff.mu.Unlock()
	if listener == nil {
		return nil, errors.New("handoff: server is not listening")
	}
	return listener.release()
}

// RestoreListener takes a released socket back when the new process failed
// before taking over.
func (s *Server) RestoreListener(file *os.File) error {
	s.handoff.mu.Lock()
	listener := s.handoff.listener
	s.handoff.mu.Unlock()
	if listener == nil {
		_ = file.Close()
		return errors.New("handoff: server is not listening")
	}
	return listener.restore(file)
}

// handoffListener wraps the listening socket so it can be given away and,
// if the takeover fails, taken back, without Accept reporting an error that
// would stop the server.
type handoffListener struct {
	mu       sync.Mutex
	inner    net.Listener
	released bool
	resume   chan struct{}
	stop     chan struct{}
	once     sync.Once
}

func newHandoffListener(inner net.Listener) *handoffListener {
	return &handoffListener{inner: inner, stop: make(chan struct{})}
}

func (l *handoffListener) Accept() (net.Conn, error) {
	for {
		l.mu.Lock()
		inner := l.inner
		l.mu.Unlock()
		conn, errAccept := inner.Accept()
		if errAccept == nil {
			return conn, nil
		}
		l.mu.Lock()
		released, resume := l.released, l.resume
		l.mu.Unlock()
		if !released {
			return nil, errAccept
		}
		select {
		case <-l.stop:
			return nil, net.ErrClosed
		case <-resume:
		}
	}
}

func (l *handoffListener) Addr() net.Addr {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Addr()
}

func (l *handoffListener) Close() error {
	l.once.Do(func() { close(l.stop) })
	l.mu.Lock()
	inner := l.inner
	l.mu.Unlock()
	return inner.Close()
}

func (l *handoffListener) release() (*os.File, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	tcp, ok := l.inner.(*net.TCPListener)
	if !ok {
		return nil, fmt.Errorf("handoff: cannot pass a %T", l.inner)
	}
	file, errFile := tcp.File()
	if errFile != nil {
		return nil, errFile
	}
	l.released = true
	l.resume = make(chan struct{})
	_ = tcp.Close() // stops this process's Accept; the socket lives on in file
	return file, nil
}

func (l *handoffListener) restore(file *os.File) error {
	inner, errListen := net.FileListener(file)
	_ = file.Close()
	if errListen != nil {
		return errListen
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.released {
		_ = inner.Close()
		return errors.New("handoff: listener was not released")
	}
	l.inner = inner
	l.released = false
	close(l.resume)
	return nil
}

// wrapHandoffConn wraps a sniffed plain-HTTP connection. Bytes the sniffer
// buffered are handed to the Conn so it knows every byte read.
func (s *Server) wrapHandoffConn(conn net.Conn, reader *bufio.Reader) net.Conn {
	if !handoff.Enabled() {
		return &bufferedConn{Conn: conn, reader: reader}
	}
	prefix, _ := reader.Peek(reader.Buffered())
	wrapped := handoff.NewConn(conn, handoff.ModeHTTPServer, prefix)
	s.trackHandoffConn(wrapped)
	return wrapped
}

// trackHandoffConn follows a Conn until it closes or leaves. An HTTP Conn
// that detaches is sent to the new process here; websocket sessions send
// themselves with their state.
func (s *Server) trackHandoffConn(conn *handoff.Conn) {
	s.handoff.mu.Lock()
	if s.handoff.conns == nil {
		s.handoff.conns = make(map[*handoff.Conn]struct{})
	}
	s.handoff.conns[conn] = struct{}{}
	s.handoff.mu.Unlock()
	go func() {
		select {
		case <-conn.Detached():
			if conn.Mode() == handoff.ModeHTTPServer {
				s.sendHandoffHTTPConn(conn)
			}
		case <-conn.Closed():
		}
		s.handoff.mu.Lock()
		delete(s.handoff.conns, conn)
		s.handoff.mu.Unlock()
	}()
}

type handoffHTTPItem struct {
	Carry []byte `json:"carry,omitempty"`
}

func (s *Server) sendHandoffHTTPConn(conn *handoff.Conn) {
	file, carry, errExtract := conn.Extract()
	if errExtract != nil {
		log.Errorf("handoff: http connection from %s: %v", conn.RemoteAddr(), errExtract)
		return
	}
	sink := handoff.CurrentSink()
	if sink == nil {
		_ = file.Close()
		log.Errorf("handoff: http connection from %s detached without a new process", conn.RemoteAddr())
		return
	}
	if errSend := sink.Send(sdkhandoff.KindConnHTTP1, "", handoffHTTPItem{Carry: carry}, file); errSend != nil {
		log.Errorf("handoff: http connection from %s: %v", conn.RemoteAddr(), errSend)
	}
}

// HandoffIdleConns detaches every HTTP connection that sits between requests
// and reports how many connections this server still holds.
func (s *Server) HandoffIdleConns() int {
	s.handoff.mu.Lock()
	conns := make([]*handoff.Conn, 0, len(s.handoff.conns))
	for conn := range s.handoff.conns {
		conns = append(conns, conn)
	}
	s.handoff.mu.Unlock()
	sink := handoff.CurrentSink()
	movable := sink != nil && sink.Supports(sdkhandoff.KindConnHTTP1)
	for _, conn := range conns {
		if movable && conn.Mode() == handoff.ModeHTTPServer {
			conn.TryDetach()
		}
	}
	// A detached Conn stays tracked until it was sent, so the caller does
	// not close the control connection under a send.
	s.handoff.mu.Lock()
	defer s.handoff.mu.Unlock()
	return len(s.handoff.conns)
}

// configureHandoff installs the connection hooks on the http.Server.
func (s *Server) configureHandoff() {
	s.server.ConnContext = handoff.ConnContext
	s.server.ConnState = func(conn net.Conn, state http.ConnState) {
		if wrapped, ok := conn.(*handoff.Conn); ok {
			wrapped.SetHTTPIdle(state == http.StateNew || state == http.StateIdle)
		}
	}
}

// handoffRerunMiddleware lets the new process re-run a request the old one
// can no longer decide on: once the request ran into ErrFrozen, nothing it
// writes reaches the client and its connection, request bytes included, is
// handed over instead.
func (s *Server) handoffRerunMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		conn := handoff.ConnFromContext(c.Request.Context())
		if conn == nil || !handoff.Enabled() {
			c.Next()
			return
		}
		ctx, marker := handoff.WithRequestMarker(c.Request.Context())
		c.Request = c.Request.WithContext(ctx)
		c.Set(handoff.MarkerGinKey, marker)
		writer := &handoffRerunWriter{ResponseWriter: c.Writer, marker: marker}
		c.Writer = writer
		c.Next()
		if marker.HitFrozen() && !writer.passed {
			if conn.TryDetachForRerun() {
				log.Infof("handoff: %s %s re-runs in the new process", c.Request.Method, c.Request.URL.Path)
			}
		}
	}
}

type handoffRerunWriter struct {
	gin.ResponseWriter
	marker *handoff.RequestMarker
	passed bool
}

func (w *handoffRerunWriter) drop() bool {
	if w.passed {
		return false
	}
	if w.marker.HitFrozen() {
		return true
	}
	w.passed = true
	return false
}

func (w *handoffRerunWriter) Write(p []byte) (int, error) {
	if w.drop() {
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}

func (w *handoffRerunWriter) WriteString(text string) (int, error) {
	if w.drop() {
		return len(text), nil
	}
	return w.ResponseWriter.WriteString(text)
}

func (w *handoffRerunWriter) WriteHeaderNow() {
	if w.drop() {
		return
	}
	w.ResponseWriter.WriteHeaderNow()
}

func (w *handoffRerunWriter) Flush() {
	if w.drop() {
		return
	}
	w.ResponseWriter.Flush()
}

// AdoptHandoffItem takes over a connection sent by the old process.
func (s *Server) AdoptHandoffItem(ctx context.Context, msg sdkhandoff.Message, files []*os.File) error {
	switch msg.Kind {
	case sdkhandoff.KindConnHTTP1:
		if len(files) != 1 {
			closeHandoffFiles(files)
			return errors.New("handoff: http connection item needs one socket")
		}
		var item handoffHTTPItem
		if len(msg.Data) > 0 {
			if errDecode := msg.Decode(&item); errDecode != nil {
				closeHandoffFiles(files)
				return errDecode
			}
		}
		conn, errAdopt := handoff.AdoptHTTP(files[0], item.Carry)
		if errAdopt != nil {
			return errAdopt
		}
		s.trackHandoffConn(conn)
		s.listenerMu.Lock()
		httpListener := s.muxHTTPListener
		s.listenerMu.Unlock()
		if httpListener == nil {
			_ = conn.Close()
			return errors.New("handoff: server is not serving yet")
		}
		return httpListener.Put(conn)
	case sdkhandoff.KindConnWSResponses:
		conn, req, errAdopt := openai.AdoptResponsesWebsocket(ctx, msg, files)
		if errAdopt != nil {
			return errAdopt
		}
		s.trackHandoffConn(conn)
		go func() {
			writer := handoff.NewHijackWriter(conn)
			s.engine.ServeHTTP(writer, req)
			if !writer.Hijacked() {
				// The handler refused the session (for example its API key
				// is gone); the client sees the socket close.
				_ = conn.Close()
			}
		}()
		return nil
	default:
		closeHandoffFiles(files)
		return fmt.Errorf("handoff: cannot adopt %s", msg.Kind)
	}
}

func closeHandoffFiles(files []*os.File) {
	for _, file := range files {
		_ = file.Close()
	}
}
