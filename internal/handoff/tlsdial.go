package handoff

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var enabled atomic.Bool

// SetEnabled turns on handoff support for this process. Without it every
// hook in this package is a no-op and the proxy behaves as upstream's.
func SetEnabled(on bool) { enabled.Store(on) }

// Enabled reports whether this process can hand off or take over.
func Enabled() bool { return enabled.Load() }

// InstallTLSDial makes a websocket dialer do its own TLS so the plaintext
// stream sits in a Conn that can later be detached and relayed. Proxies set on
// the dialer keep working: HTTP CONNECT is done here, other schemes go through
// the dialer's NetDialContext as before.
func InstallTLSDial(d *websocket.Dialer) {
	if d == nil || !Enabled() || d.NetDialTLSContext != nil {
		return
	}
	proxyFor := d.Proxy
	base := d.NetDialContext
	if base == nil {
		base = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	tlsConfig := d.TLSClientConfig
	d.Proxy = nil
	d.NetDialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, errSplit := net.SplitHostPort(addr)
		if errSplit != nil {
			return nil, errSplit
		}
		var proxyURL *url.URL
		if proxyFor != nil {
			parsed, errProxy := proxyFor(&http.Request{URL: &url.URL{Scheme: "https", Host: addr}})
			if errProxy != nil {
				return nil, errProxy
			}
			proxyURL = parsed
		}
		var raw net.Conn
		var errDial error
		if proxyURL != nil {
			raw, errDial = dialConnect(ctx, base, proxyURL, addr)
		} else {
			raw, errDial = base(ctx, network, addr)
		}
		if errDial != nil {
			return nil, errDial
		}
		cfg := &tls.Config{}
		if tlsConfig != nil {
			cfg = tlsConfig.Clone()
		}
		if cfg.ServerName == "" {
			cfg.ServerName = host
		}
		cfg.NextProtos = []string{"http/1.1"}
		tlsConn := tls.Client(raw, cfg)
		if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
			_ = raw.Close()
			return nil, errHandshake
		}
		return NewConn(tlsConn, ModeWSClientHandshake, nil), nil
	}
}

// dialConnect opens a tunnel through an HTTP proxy.
func dialConnect(ctx context.Context, base func(context.Context, string, string) (net.Conn, error), proxyURL *url.URL, addr string) (net.Conn, error) {
	if proxyURL.Scheme != "http" {
		return nil, fmt.Errorf("handoff: unsupported proxy scheme %q for websocket", proxyURL.Scheme)
	}
	proxyAddr := proxyURL.Host
	if proxyURL.Port() == "" {
		proxyAddr = net.JoinHostPort(proxyURL.Hostname(), "80")
	}
	conn, errDial := base(ctx, "tcp", proxyAddr)
	if errDial != nil {
		return nil, errDial
	}
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: addr},
		Host:   addr,
		Header: make(http.Header),
	}
	if user := proxyURL.User; user != nil {
		password, _ := user.Password()
		credentials := base64.StdEncoding.EncodeToString([]byte(user.Username() + ":" + password))
		req.Header.Set("Proxy-Authorization", "Basic "+credentials)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer func() { _ = conn.SetDeadline(time.Time{}) }()
	}
	if errWrite := req.Write(conn); errWrite != nil {
		_ = conn.Close()
		return nil, errWrite
	}
	// Read the reply byte-exact: anything after it belongs to TLS.
	resp, errRead := http.ReadResponse(bufio.NewReaderSize(&oneByteReader{conn}, 1), req)
	if errRead != nil {
		_ = conn.Close()
		return nil, errRead
	}
	// The body is not read or closed: a CONNECT reply has none, and closing
	// would try to read the tunnel.
	if resp.StatusCode != http.StatusOK {
		_ = conn.Close()
		return nil, fmt.Errorf("handoff: proxy CONNECT failed: %s", resp.Status)
	}
	return conn, nil
}

// oneByteReader keeps bufio from reading past the proxy's reply.
type oneByteReader struct{ conn net.Conn }

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.conn.Read(p)
}
