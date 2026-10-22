package handoff

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func wsFrame(fin bool, opcode byte, masked bool, payload []byte) []byte {
	var out bytes.Buffer
	first := opcode
	if fin {
		first |= 0x80
	}
	out.WriteByte(first)
	maskBit := byte(0)
	if masked {
		maskBit = 0x80
	}
	switch {
	case len(payload) < 126:
		out.WriteByte(maskBit | byte(len(payload)))
	case len(payload) <= 0xffff:
		out.WriteByte(maskBit | 126)
		_ = binary.Write(&out, binary.BigEndian, uint16(len(payload)))
	default:
		out.WriteByte(maskBit | 127)
		_ = binary.Write(&out, binary.BigEndian, uint64(len(payload)))
	}
	if masked {
		out.Write([]byte{1, 2, 3, 4})
	}
	out.Write(payload)
	return out.Bytes()
}

func TestFrameTrackerStopsAtEveryFrameEnd(t *testing.T) {
	frames := [][]byte{
		wsFrame(true, 1, true, []byte("hello")),
		wsFrame(false, 1, false, bytes.Repeat([]byte("a"), 300)),
		wsFrame(true, 9, true, nil), // ping inside a fragmented message
		wsFrame(true, 0, false, bytes.Repeat([]byte("b"), 70000)),
		wsFrame(true, 2, true, []byte{}),
	}
	stream := bytes.Join(frames, nil)
	var tracker frameTracker
	boundaries := []bool{true, false, false, true, true}
	for i, frame := range frames {
		if got := tracker.limit(stream); got != len(frame) {
			t.Fatalf("frame %d: limit = %d, want %d", i, got, len(frame))
		}
		// Deliver in uneven pieces to exercise partial headers.
		for _, piece := range [][]byte{frame[:1], frame[1:]} {
			tracker.feed(piece)
			if len(piece) > 0 && tracker.atBoundary() && piece[len(piece)-1] != frame[len(frame)-1] {
				t.Fatalf("frame %d: boundary reported mid-frame", i)
			}
		}
		if tracker.atBoundary() != boundaries[i] {
			t.Fatalf("frame %d: atBoundary = %v, want %v", i, tracker.atBoundary(), boundaries[i])
		}
		stream = stream[len(frame):]
	}
	if tracker.frames != len(frames) {
		t.Fatalf("frames = %d", tracker.frames)
	}
}

func TestHTTPFramerBoundaries(t *testing.T) {
	var framer httpFramer
	first := "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello"
	second := "POST /b HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3;ext=1\r\nabc\r\n0\r\nX-T: 1\r\n\r\n"
	third := "GET /c HTTP/1.1\r\nHost: x\r\n\r\n"
	for _, req := range []string{first, second, third} {
		for i := 0; i < len(req); i++ {
			if framer.atBoundary() != (i == 0) {
				t.Fatalf("%q: boundary at byte %d = %v", req[:12], i, framer.atBoundary())
			}
			framer.feed([]byte{req[i]})
		}
		if !framer.atBoundary() {
			t.Fatalf("%q: not at boundary after the request", req[:12])
		}
	}
	if framer.requests != 3 {
		t.Fatalf("requests = %d", framer.requests)
	}
	var bad httpFramer
	bad.feed([]byte("POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n"))
	if bad.atBoundary() {
		t.Fatal("unframeable request reported a boundary")
	}
}

// wrapListener wraps every accepted conn and reports it.
type wrapListener struct {
	net.Listener
	conns chan *Conn
}

func (l *wrapListener) Accept() (net.Conn, error) {
	raw, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	conn := NewConn(raw, ModeHTTPServer, nil)
	select {
	case l.conns <- conn:
	default:
	}
	return conn, nil
}

// oneConnListener serves exactly one adopted conn.
type oneConnListener struct {
	conn net.Conn
	once sync.Once
	done chan struct{}
}

func newOneConnListener(conn net.Conn) *oneConnListener {
	return &oneConnListener{conn: conn, done: make(chan struct{})}
}

func (l *oneConnListener) Accept() (net.Conn, error) {
	var conn net.Conn
	l.once.Do(func() { conn = l.conn })
	if conn != nil {
		return conn, nil
	}
	<-l.done
	return nil, net.ErrClosed
}

func (l *oneConnListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}

func (l *oneConnListener) Addr() net.Addr { return l.conn.LocalAddr() }

func connStateHook(conn net.Conn, state http.ConnState) {
	if c, ok := conn.(*Conn); ok {
		c.SetHTTPIdle(state == http.StateNew || state == http.StateIdle)
	}
}

func serve(t *testing.T, l net.Listener, handler http.Handler) *http.Server {
	t.Helper()
	server := &http.Server{Handler: handler, ConnState: connStateHook, ConnContext: ConnContext}
	go func() { _ = server.Serve(l) }()
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func reusedRequest(t *testing.T, client *http.Client, url, body string) (string, bool) {
	t.Helper()
	var reused bool
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}
	req, _ := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), http.MethodPost, url, strings.NewReader(body))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	return string(data), reused
}

func TestIdleHTTPConnMovesToAnotherServer(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &wrapListener{Listener: base, conns: make(chan *Conn, 1)}
	serve(t, wrapped, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte("old"))
	}))
	client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
	url := "http://" + base.Addr().String() + "/"

	if got, _ := reusedRequest(t, client, url, "one"); got != "old" {
		t.Fatalf("first response %q", got)
	}
	conn := <-wrapped.conns
	deadline := time.Now().Add(5 * time.Second)
	for !conn.TryDetach() {
		if time.Now().After(deadline) {
			t.Fatal("idle conn never detached")
		}
		time.Sleep(10 * time.Millisecond)
	}
	<-conn.Detached()
	file, carry, err := conn.Extract()
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := AdoptHTTP(file, carry)
	if err != nil {
		t.Fatal(err)
	}
	serve(t, newOneConnListener(adopted), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write([]byte("new:" + string(body)))
	}))
	got, reused := reusedRequest(t, client, url, "two")
	if got != "new:two" || !reused {
		t.Fatalf("second response %q reused=%v, want new:two on the same connection", got, reused)
	}
}

func TestBusyHTTPConnDoesNotDetach(t *testing.T) {
	base, _ := net.Listen("tcp", "127.0.0.1:0")
	wrapped := &wrapListener{Listener: base, conns: make(chan *Conn, 1)}
	release := make(chan struct{})
	inHandler := make(chan struct{})
	serve(t, wrapped, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(inHandler)
		<-release
		_, _ = w.Write([]byte("done"))
	}))
	done := make(chan string)
	go func() {
		resp, err := http.Post("http://"+base.Addr().String()+"/", "text/plain", strings.NewReader("x"))
		if err != nil {
			done <- err.Error()
			return
		}
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		done <- string(data)
	}()
	<-inHandler
	conn := <-wrapped.conns
	if conn.TryDetach() {
		t.Fatal("detached while a request was being handled")
	}
	close(release)
	if got := <-done; got != "done" {
		t.Fatalf("response %q", got)
	}
}

func TestRequestIsRerunByAnotherServer(t *testing.T) {
	base, _ := net.Listen("tcp", "127.0.0.1:0")
	wrapped := &wrapListener{Listener: base, conns: make(chan *Conn, 1)}
	var current atomic.Pointer[Conn]
	moved := make(chan struct{})
	serve(t, wrapped, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		conn := ConnFromContext(r.Context())
		current.Store(conn)
		if !conn.TryDetachForRerun() {
			t.Error("rerun detach refused")
		}
		close(moved)
		// Whatever the old handler writes now goes nowhere.
		_, _ = w.Write([]byte("old error"))
	}))
	result := make(chan string)
	go func() {
		resp, err := http.Post("http://"+base.Addr().String()+"/x", "text/plain", strings.NewReader("payload"))
		if err != nil {
			result <- err.Error()
			return
		}
		data, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		result <- string(data)
	}()
	<-moved
	conn := current.Load()
	<-conn.Detached()
	file, carry, err := conn.Extract()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(carry, []byte("POST /x HTTP/1.1")) || !bytes.HasSuffix(carry, []byte("payload")) {
		t.Fatalf("carry is not the whole request: %q", carry)
	}
	adopted, err := AdoptHTTP(file, carry)
	if err != nil {
		t.Fatal(err)
	}
	serve(t, newOneConnListener(adopted), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write([]byte("rerun:" + string(body)))
	}))
	if got := <-result; got != "rerun:payload" {
		t.Fatalf("client got %q", got)
	}
}

func TestClientWebsocketMovesBetweenHandlers(t *testing.T) {
	base, _ := net.Listen("tcp", "127.0.0.1:0")
	wrapped := &wrapListener{Listener: base, conns: make(chan *Conn, 1)}
	upgrader := websocket.Upgrader{}
	var upgradeReq atomic.Pointer[http.Request]
	oldDone := make(chan error, 1)
	serve(t, wrapped, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgradeReq.Store(r.Clone(context.Background()))
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			oldDone <- err
			return
		}
		for {
			kind, msg, errRead := ws.ReadMessage()
			if errRead != nil {
				oldDone <- errRead
				_ = ws.Close()
				return
			}
			_ = ws.WriteMessage(kind, append([]byte("old:"), msg...))
		}
	}))
	client, _, err := websocket.DefaultDialer.Dial("ws://"+base.Addr().String()+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	roundTrip := func(msg string) string {
		if errWrite := client.WriteMessage(websocket.TextMessage, []byte(msg)); errWrite != nil {
			t.Fatal(errWrite)
		}
		_, reply, errRead := client.ReadMessage()
		if errRead != nil {
			t.Fatal(errRead)
		}
		return string(reply)
	}
	if got := roundTrip("1"); got != "old:1" {
		t.Fatalf("got %q", got)
	}
	conn := <-wrapped.conns
	if conn.Mode() != ModeWSServer {
		t.Fatalf("mode = %v after the upgrade", conn.Mode())
	}
	for !conn.TryDetach() {
		time.Sleep(5 * time.Millisecond)
	}
	<-conn.Detached()
	if errOld := <-oldDone; errOld == nil {
		t.Fatal("old handler kept reading")
	}
	file, carry, err := conn.Extract()
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := AdoptServerWebsocket(file, carry)
	if err != nil {
		t.Fatal(err)
	}
	writer := NewHijackWriter(adopted)
	ws, err := upgrader.Upgrade(writer, upgradeReq.Load(), nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			kind, msg, errRead := ws.ReadMessage()
			if errRead != nil {
				return
			}
			_ = ws.WriteMessage(kind, append([]byte("new:"), msg...))
		}
	}()
	if got := roundTrip("2"); got != "new:2" {
		t.Fatalf("got %q after the move", got)
	}
}

func TestUpstreamWebsocketRelaysAndChains(t *testing.T) {
	SetEnabled(true)
	defer SetEnabled(false)
	var connIDs sync.Map
	var nextID atomic.Int64
	upgrader := websocket.Upgrader{EnableCompression: true}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		id := nextID.Add(1)
		connIDs.Store(ws.NetConn().RemoteAddr().String(), id)
		for {
			kind, msg, errRead := ws.ReadMessage()
			if errRead != nil {
				return
			}
			_ = ws.WriteMessage(kind, []byte(fmt.Sprintf("conn%d:%s", id, msg)))
		}
	}))
	defer server.Close()
	pool := server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	dialer := &websocket.Dialer{TLSClientConfig: &tls.Config{RootCAs: pool}, EnableCompression: true}
	InstallTLSDial(dialer)
	wsURL := "wss" + strings.TrimPrefix(server.URL, "https") + "/ws"
	ws, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip := func(conn *websocket.Conn, msg string) string {
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(msg)); errWrite != nil {
			t.Fatal(errWrite)
		}
		_, reply, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatal(errRead)
		}
		return string(reply)
	}
	if got := roundTrip(ws, "1"); got != "conn1:1" {
		t.Fatalf("got %q", got)
	}
	inner, ok := ws.NetConn().(*Conn)
	if !ok {
		t.Fatalf("dialer did not install the handoff conn: %T", ws.NetConn())
	}
	response := inner.HandshakeResponse()
	if !bytes.HasPrefix(response, []byte("HTTP/1.1 101")) {
		t.Fatalf("recorded handshake %q", response)
	}

	move := func(conn *websocket.Conn, inner *Conn) (*websocket.Conn, *Conn) {
		readerDone := make(chan struct{})
		go func() {
			_, _, _ = conn.ReadMessage() // a reader parked between messages
			close(readerDone)
		}()
		time.Sleep(20 * time.Millisecond)
		if !inner.TryDetach() {
			t.Fatal("idle upstream did not detach")
		}
		<-inner.Detached()
		<-readerDone
		file, carry, errTransfer := Transfer(inner)
		if errTransfer != nil {
			t.Fatal(errTransfer)
		}
		_ = conn.Close() // must not close the transferred socket
		next, nextInner, errAdopt := AdoptClientWebsocket(context.Background(), file, carry, response, wsURL, true)
		if errAdopt != nil {
			t.Fatal(errAdopt)
		}
		return next, nextInner
	}
	ws2, inner2 := move(ws, inner) // TLS -> relay pump
	if got := roundTrip(ws2, "2"); got != "conn1:2" {
		t.Fatalf("after the relay got %q, want the same upstream connection", got)
	}
	ws3, _ := move(ws2, inner2) // unix socket -> passed on as is
	if got := roundTrip(ws3, "3"); got != "conn1:3" {
		t.Fatalf("after chaining got %q", got)
	}
	_ = ws3.Close()
	waited := make(chan struct{})
	go func() { WaitRelays(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("relay pump did not end after the last owner closed")
	}
	if nextID.Load() != 1 {
		t.Fatalf("upstream saw %d connections", nextID.Load())
	}
}
