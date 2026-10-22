package executor

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/handoff"
	sdkhandoff "github.com/router-for-me/CLIProxyAPI/v8/sdk/handoff"
)

// Codex upstream websockets survive a binary handoff: the old process gives
// the session's plaintext stream to the new one (relaying TLS for it), and
// the new process puts it back into the same session, where the executor's
// normal reuse path finds it.
func init() {
	handoff.AddCapability(sdkhandoff.KindRelayPlain)
	handoff.RegisterUpstream("codex", handoff.UpstreamHooks{
		Export: exportCodexUpstream,
		Adopt:  adoptCodexUpstream,
	})
}

func exportCodexUpstream(sessionID string) (*handoff.Conn, handoff.UpstreamState, bool) {
	store := globalCodexWebsocketSessionStore
	store.mu.Lock()
	sess := store.sessions[strings.TrimSpace(sessionID)]
	store.mu.Unlock()
	if sess == nil {
		return nil, handoff.UpstreamState{}, false
	}
	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	if sess.conn == nil {
		return nil, handoff.UpstreamState{}, false
	}
	inner, ok := handoff.Unwrap(sess.conn.NetConn())
	if !ok {
		return nil, handoff.UpstreamState{}, false
	}
	return inner, handoff.UpstreamState{
		Provider:    "codex",
		SessionID:   sess.sessionID,
		AuthID:      sess.authID,
		WSURL:       sess.wsURL,
		ProxyURL:    sess.proxyURL,
		Response101: inner.HandshakeResponse(),
	}, true
}

func adoptCodexUpstream(ctx context.Context, state handoff.UpstreamState, file *os.File, carry []byte) error {
	conn, _, errAdopt := handoff.AdoptClientWebsocket(ctx, file, carry, state.Response101, state.WSURL, true)
	if errAdopt != nil {
		return errAdopt
	}
	// Same as a fresh dial: compression negotiated, outbound uncompressed.
	conn.EnableWriteCompression(false)
	e := &CodexWebsocketsExecutor{store: globalCodexWebsocketSessionStore}
	sess := e.getOrCreateSession(state.SessionID)
	if sess == nil {
		_ = conn.Close()
		return errors.New("handoff: codex upstream without a session id")
	}
	sess.connMu.Lock()
	if sess.conn != nil {
		sess.connMu.Unlock()
		_ = conn.Close()
		return errors.New("handoff: codex session " + state.SessionID + " already has an upstream")
	}
	sess.conn = conn
	sess.connCloser = newWebsocketConnectionCloser(conn)
	sess.wsURL = state.WSURL
	sess.authID = state.AuthID
	sess.proxyURL = state.ProxyURL
	sess.readerConn = conn
	sess.connMu.Unlock()
	// The old process kept reading between turns (pings, idle timeout); keep
	// doing so. The next request reuses conn like one dialled here.
	sess.configureConn(conn)
	go e.readUpstreamLoop(sess, conn)
	return nil
}
