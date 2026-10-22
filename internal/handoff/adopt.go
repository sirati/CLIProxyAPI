package handoff

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

// FileConn turns a received socket file into a net.Conn and closes the file.
func FileConn(file *os.File) (net.Conn, error) {
	conn, errConn := net.FileConn(file)
	_ = file.Close()
	return conn, errConn
}

// AdoptHTTP wraps a received HTTP/1.1 client socket. carry holds bytes the
// old process read but net/http never saw; they are served first.
func AdoptHTTP(file *os.File, carry []byte) (*Conn, error) {
	raw, errConn := FileConn(file)
	if errConn != nil {
		return nil, errConn
	}
	return NewConn(raw, ModeHTTPServer, carry), nil
}

// AdoptServerWebsocket wraps a received client websocket socket. The Conn
// drops the 101 the handler writes when it replays the upgrade request.
func AdoptServerWebsocket(file *os.File, carry []byte) (*Conn, error) {
	raw, errConn := FileConn(file)
	if errConn != nil {
		return nil, errConn
	}
	c := NewConn(raw, ModeWSServer, carry)
	c.httpIdle = false
	c.swallow101 = true
	return c, nil
}

// HijackWriter is the http.ResponseWriter used to replay an adopted
// websocket's upgrade request through the normal handler chain. Hijack hands
// out the adopted Conn; anything written before that is discarded.
type HijackWriter struct {
	conn   *Conn
	header http.Header
	mu     sync.Mutex
	done   bool
	status int
}

// NewHijackWriter returns a writer whose Hijack returns conn.
func NewHijackWriter(conn *Conn) *HijackWriter {
	return &HijackWriter{conn: conn, header: make(http.Header)}
}

// Header implements http.ResponseWriter.
func (w *HijackWriter) Header() http.Header { return w.header }

// Write implements http.ResponseWriter. A handler that answers instead of
// upgrading (for example, the API key was removed) gets its reply discarded
// and the socket is closed when ServeHTTP returns.
func (w *HijackWriter) Write(p []byte) (int, error) { return len(p), nil }

// WriteHeader implements http.ResponseWriter.
func (w *HijackWriter) WriteHeader(status int) { w.status = status }

// Flush implements http.Flusher.
func (w *HijackWriter) Flush() {}

// Hijack implements http.Hijacker.
func (w *HijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return nil, nil, errors.New("handoff: connection already hijacked")
	}
	w.done = true
	rw := bufio.NewReadWriter(bufio.NewReaderSize(w.conn, 4096), bufio.NewWriter(w.conn))
	return w.conn, rw, nil
}

// Hijacked reports whether the handler took the connection.
func (w *HijackWriter) Hijacked() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.done
}

// AdoptClientWebsocket rebuilds the client side of an upstream websocket from
// a received plaintext stream. response101 is the upstream's original 101;
// gorilla runs a normal handshake over the stream, which is answered locally
// with that response and an Accept matching the new key, so nothing reaches
// the upstream until the first real message.
func AdoptClientWebsocket(ctx context.Context, file *os.File, carry, response101 []byte, wsURL string, enableCompression bool) (*websocket.Conn, *Conn, error) {
	raw, errConn := FileConn(file)
	if errConn != nil {
		return nil, nil, errConn
	}
	inner := NewConn(raw, ModeWSClient, carry)
	inner.httpIdle = false
	// The next process replays the same handshake.
	inner.handshakeResponse = append([]byte(nil), response101...)
	local := &handshakeAnswerConn{Conn: inner, response: response101}
	dialer := websocket.Dialer{
		NetDialContext:    func(context.Context, string, string) (net.Conn, error) { return local, nil },
		NetDialTLSContext: func(context.Context, string, string) (net.Conn, error) { return local, nil },
		EnableCompression: enableCompression,
		Proxy:             nil,
	}
	conn, _, errDial := dialer.DialContext(ctx, wsURL, nil)
	if errDial != nil {
		_ = raw.Close()
		return nil, nil, fmt.Errorf("handoff: replay upstream handshake: %w", errDial)
	}
	return conn, inner, nil
}

// handshakeAnswerConn swallows the first write (the client's upgrade request)
// and answers it with the recorded 101, re-keyed for the new request.
type handshakeAnswerConn struct {
	*Conn
	mu       sync.Mutex
	response []byte
	answer   []byte
	answered bool
}

func (c *handshakeAnswerConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if !c.answered {
		c.answered = true
		key := headerValue(p, "Sec-WebSocket-Key")
		c.answer = rekey101(c.response, key)
		c.mu.Unlock()
		return len(p), nil
	}
	c.mu.Unlock()
	return c.Conn.Write(p)
}

func (c *handshakeAnswerConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.answer) > 0 {
		n := copy(p, c.answer)
		c.answer = c.answer[n:]
		c.mu.Unlock()
		return n, nil
	}
	c.mu.Unlock()
	return c.Conn.Read(p)
}

func headerValue(raw []byte, name string) string {
	for _, line := range strings.Split(string(raw), "\r\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(strings.TrimSpace(key), name) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// rekey101 replaces Sec-WebSocket-Accept in a recorded 101 response.
func rekey101(response []byte, key string) []byte {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	head, _, _ := bytes.Cut(response, []byte("\r\n\r\n"))
	var out bytes.Buffer
	replaced := false
	for i, line := range strings.Split(string(head), "\r\n") {
		if i > 0 {
			if key, _, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(key), "Sec-WebSocket-Accept") {
				line = "Sec-WebSocket-Accept: " + accept
				replaced = true
			}
		}
		out.WriteString(line)
		out.WriteString("\r\n")
	}
	if !replaced {
		out.WriteString("Sec-WebSocket-Accept: " + accept + "\r\n")
	}
	out.WriteString("\r\n")
	return out.Bytes()
}
