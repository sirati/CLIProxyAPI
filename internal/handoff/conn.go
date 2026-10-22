package handoff

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// Mode says which protocol a Conn is framing.
type Mode int

const (
	// ModeHTTPServer is the server side of HTTP/1.1. A 101 response written
	// through the Conn switches it to ModeWSServer.
	ModeHTTPServer Mode = iota
	// ModeWSServer is the server side of a websocket (masked client frames).
	ModeWSServer
	// ModeWSClientHandshake is the client side of an upstream websocket until
	// the 101 response has been read; it then becomes ModeWSClient.
	ModeWSClientHandshake
	// ModeWSClient is the client side of an upstream websocket.
	ModeWSClient
)

// Conn wraps a socket so it can be taken away from its current reader at a
// protocol boundary and handed to another process.
//
// It owns the blocking read: data is read into its own buffer and handed to
// the caller only up to the end of the current request (HTTP) or frame
// (websocket), so the caller never holds bytes the Conn does not know about.
// Detach stops delivering at a boundary, wakes the blocked read, and from then
// on Read returns io.EOF, Write fails and Close leaves the socket open, so the
// code that used the Conn unwinds without touching the client.
type Conn struct {
	raw net.Conn

	mu   sync.Mutex
	cond *sync.Cond
	mode Mode

	pending []byte // read from raw, not delivered yet

	http     httpFramer
	frames   frameTracker
	request  []byte // bytes of the current HTTP request, for a re-run
	httpIdle bool   // net/http is between requests (StateNew or StateIdle)
	written  int64  // bytes written since the current HTTP request began

	handshakeRequest  []byte // client mode: the upgrade request written
	handshakeResponse []byte // client mode: the 101 response read

	reading      bool
	userDeadline time.Time
	detachReq    bool
	rerun        bool // detach mid-request because the new process re-runs it
	detached     bool
	detachedCh   chan struct{}
	closed       bool
	closeCh      chan struct{}
	writing      int

	// swallow101 drops the next written 101 response: an adopted websocket
	// replays its upgrade through the normal handler, but the client already
	// has its handshake.
	swallow101 bool

	// OnClose runs once when the user closes a Conn that was never detached.
	OnClose func()
}

// ErrDetached is returned by Write after the Conn was handed away.
var ErrDetached = errors.New("handoff: connection was handed to another process")

// NewConn wraps raw. prefix holds bytes already read from raw by a protocol
// sniffer; they are delivered first.
func NewConn(raw net.Conn, mode Mode, prefix []byte) *Conn {
	c := &Conn{
		raw:        raw,
		mode:       mode,
		pending:    append([]byte(nil), prefix...),
		httpIdle:   true,
		detachedCh: make(chan struct{}),
		closeCh:    make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// Raw returns the wrapped socket.
func (c *Conn) Raw() net.Conn { return c.raw }

// Mode returns the current framing mode.
func (c *Conn) Mode() Mode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mode
}

// SetHTTPIdle records net/http's view: true for StateNew/StateIdle, false for
// StateActive. A request that ends lets the next one start capturing.
func (c *Conn) SetHTTPIdle(idle bool) {
	c.mu.Lock()
	c.httpIdle = idle
	if idle {
		c.written = 0
	}
	c.mu.Unlock()
}

// Read implements net.Conn.
func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.detached {
			return 0, io.EOF
		}
		if c.detachReq && c.detachableLocked() {
			c.finishDetachLocked()
			return 0, io.EOF
		}
		if len(c.pending) > 0 && !c.detachReq {
			n := c.deliverableLocked(len(p))
			if n > 0 {
				copy(p, c.pending[:n])
				c.consumeLocked(p[:n])
				c.pending = c.pending[n:]
				return n, nil
			}
		}
		if c.closed {
			return 0, net.ErrClosed
		}
		if c.reading {
			// Another goroutine owns the socket read; wait for it.
			c.cond.Wait()
			continue
		}
		c.reading = true
		buf := make([]byte, max(len(p), 4096))
		c.mu.Unlock()
		n, errRead := c.raw.Read(buf)
		c.mu.Lock()
		c.reading = false
		c.cond.Broadcast()
		if n > 0 {
			c.pending = append(c.pending, buf[:n]...)
		}
		if errRead == nil {
			continue
		}
		if isTimeout(errRead) {
			if c.detachReq {
				continue
			}
			if !c.userDeadline.IsZero() && !time.Now().Before(c.userDeadline) {
				return 0, errRead
			}
			// Our own wake-up after a cancelled detach: restore and retry.
			_ = c.raw.SetReadDeadline(c.userDeadline)
			continue
		}
		if len(c.pending) > 0 && !c.detachReq {
			// Deliver what arrived before the error first.
			continue
		}
		return 0, errRead
	}
}

// deliverableLocked returns how many pending bytes the caller may take now.
func (c *Conn) deliverableLocked(want int) int {
	n := min(want, len(c.pending))
	switch c.mode {
	case ModeWSServer, ModeWSClient:
		n = min(n, c.frames.limit(c.pending[:n]))
	case ModeWSClientHandshake:
		// Stop right after the response headers so the websocket reader
		// never buffers frames together with the handshake.
		seen := len(c.handshakeResponse)
		window := append(append([]byte(nil), c.handshakeResponse[max(0, seen-3):]...), c.pending[:n]...)
		if idx := bytes.Index(window, []byte("\r\n\r\n")); idx >= 0 {
			n = idx + 4 - min(seen, 3)
		}
	}
	return n
}

func (c *Conn) consumeLocked(b []byte) {
	switch c.mode {
	case ModeHTTPServer:
		if c.http.atBoundary() {
			c.request = c.request[:0]
		}
		c.request = append(c.request, b...)
		c.http.feed(b)
	case ModeWSServer, ModeWSClient:
		c.frames.feed(b)
	case ModeWSClientHandshake:
		c.handshakeResponse = append(c.handshakeResponse, b...)
		if bytes.HasSuffix(c.handshakeResponse, []byte("\r\n\r\n")) {
			if bytes.HasPrefix(c.handshakeResponse, []byte("HTTP/1.1 101")) {
				c.mode = ModeWSClient
			}
		}
	}
}

// detachableLocked reports whether no part of a protocol unit has been handed
// to the reader and no write is in flight.
func (c *Conn) detachableLocked() bool {
	if c.writing > 0 {
		return false
	}
	switch c.mode {
	case ModeHTTPServer:
		if c.rerun {
			return c.written == 0
		}
		return c.httpIdle && c.http.atBoundary()
	case ModeWSServer, ModeWSClient:
		return c.frames.atBoundary()
	default:
		return false
	}
}

func (c *Conn) finishDetachLocked() {
	if c.detached {
		return
	}
	c.detached = true
	close(c.detachedCh)
	c.cond.Broadcast()
}

// TryDetach asks the Conn to stop at the current boundary. It returns false if
// a unit is in progress; the caller retries at the next idle point. On true,
// Detached is closed once the reader has been released.
func (c *Conn) TryDetach() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tryDetachLocked(false)
}

// TryDetachForRerun detaches an HTTP connection in the middle of a request that
// has not written a byte, so the new process can run the request again.
func (c *Conn) TryDetachForRerun() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mode != ModeHTTPServer || c.http.opaque || !c.http.atBoundary() || c.written > 0 {
		return false
	}
	return c.tryDetachLocked(true)
}

func (c *Conn) tryDetachLocked(rerun bool) bool {
	if c.detached {
		return true
	}
	if c.closed {
		return false
	}
	c.rerun = rerun
	if !c.detachableLocked() {
		c.rerun = false
		return false
	}
	c.detachReq = true
	if c.reading {
		// Wake the blocked read; it sees detachReq and stops.
		_ = c.raw.SetReadDeadline(time.Unix(1, 0))
	} else {
		c.finishDetachLocked()
	}
	return true
}

// Detached is closed when the Conn has been taken away from its reader.
func (c *Conn) Detached() <-chan struct{} { return c.detachedCh }

// IsDetached reports whether the Conn was handed away.
func (c *Conn) IsDetached() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.detached
}

// Extract returns the socket as a file plus every byte read from it that the
// next owner must see first: for a re-run, the whole current request.
// It closes the wrapped socket; the returned file is an independent dup.
func (c *Conn) Extract() (*os.File, []byte, error) {
	c.mu.Lock()
	if !c.detached {
		c.mu.Unlock()
		return nil, nil, errors.New("handoff: extract before detach")
	}
	for c.reading {
		c.cond.Wait()
	}
	carry := append([]byte(nil), c.pending...)
	if c.rerun {
		carry = append(append([]byte(nil), c.request...), carry...)
	}
	c.mu.Unlock()
	_ = c.raw.SetReadDeadline(time.Time{})
	file, errFile := fileOf(c.raw)
	if errFile != nil {
		return nil, nil, errFile
	}
	_ = c.raw.Close()
	return file, carry, nil
}

// HandshakeResponse returns the raw 101 response of a client-mode Conn.
func (c *Conn) HandshakeResponse() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.handshakeResponse...)
}

// Write implements net.Conn.
func (c *Conn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.detached || c.detachReq {
		c.mu.Unlock()
		return 0, ErrDetached
	}
	if c.swallow101 && bytes.HasPrefix(p, []byte("HTTP/1.1 101 ")) {
		c.swallow101 = false
		c.mu.Unlock()
		return len(p), nil
	}
	c.writing++
	switch c.mode {
	case ModeHTTPServer:
		c.written += int64(len(p))
		if bytes.HasPrefix(p, []byte("HTTP/1.1 101 ")) && c.http.atBoundary() {
			// The upgrade request has been read in full, so every later byte
			// is websocket framing.
			c.mode = ModeWSServer
		}
	case ModeWSClientHandshake:
		if len(c.handshakeRequest) == 0 {
			c.handshakeRequest = append([]byte(nil), p...)
		}
	}
	c.mu.Unlock()
	n, errWrite := c.raw.Write(p)
	c.mu.Lock()
	c.writing--
	c.cond.Broadcast()
	c.mu.Unlock()
	return n, errWrite
}

// Close implements net.Conn. After a detach it only marks the Conn closed.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.closeCh)
	detached := c.detached || c.detachReq
	if c.detachReq {
		// The reader gave up before seeing the wake-up; the socket is ours.
		c.finishDetachLocked()
	}
	onClose := c.OnClose
	c.cond.Broadcast()
	c.mu.Unlock()
	if detached {
		return nil
	}
	if onClose != nil {
		onClose()
	}
	return c.raw.Close()
}

// Closed is closed once the user closed the Conn.
func (c *Conn) Closed() <-chan struct{} { return c.closeCh }

// LocalAddr implements net.Conn.
func (c *Conn) LocalAddr() net.Addr { return c.raw.LocalAddr() }

// RemoteAddr implements net.Conn.
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }

// SetDeadline implements net.Conn.
func (c *Conn) SetDeadline(t time.Time) error {
	if errRead := c.SetReadDeadline(t); errRead != nil {
		return errRead
	}
	return c.raw.SetWriteDeadline(t)
}

// SetReadDeadline implements net.Conn. The Conn keeps the caller's deadline
// separate from its own wake-ups.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.userDeadline = t
	if c.detachReq {
		return nil
	}
	return c.raw.SetReadDeadline(t)
}

// SetWriteDeadline implements net.Conn.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }

// NetConn returns the wrapped socket, for code that inspects it.
func (c *Conn) NetConn() net.Conn { return c.raw }

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func fileOf(conn net.Conn) (*os.File, error) {
	type filer interface{ File() (*os.File, error) }
	if f, ok := conn.(filer); ok {
		return f.File()
	}
	return nil, errors.New("handoff: socket cannot be passed to another process")
}

type connContextKey struct{}

// ContextWithConn stores the Conn a request arrived on.
func ContextWithConn(ctx context.Context, c *Conn) context.Context {
	return context.WithValue(ctx, connContextKey{}, c)
}

// ConnFromContext returns the Conn a request arrived on, or nil.
func ConnFromContext(ctx context.Context) *Conn {
	if ctx == nil {
		return nil
	}
	c, _ := ctx.Value(connContextKey{}).(*Conn)
	return c
}

// ConnContext is an http.Server.ConnContext that exposes the Conn to handlers.
func ConnContext(ctx context.Context, conn net.Conn) context.Context {
	if c, ok := conn.(*Conn); ok {
		return ContextWithConn(ctx, c)
	}
	return ctx
}
