package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/handoff"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkhandoff "github.com/router-for-me/CLIProxyAPI/v8/sdk/handoff"
	log "github.com/sirupsen/logrus"
)

// A Responses websocket session moves to a new process between turns: the
// client socket, the upstream websocket (relayed by the old process) and the
// handler's per-session variables. The handler keeps its locals; it only
// hands their addresses over (bindResponsesWebsocketHandoff) and marks where
// it waits for the next message (park).

func init() {
	handoff.AddCapability(sdkhandoff.KindConnWSResponses)
}

// responsesWebsocketHandoffVars points at ResponsesWebsocket's per-session
// locals. The guard test keeps it in step with the handler.
type responsesWebsocketHandoffVars struct {
	SessionID                      *string
	SessionKey                     *string
	LastRequest                    *[]byte
	LastResponseOutput             *[]byte
	ObservedCompaction             *responsesWebsocketObservedCompactionState
	LastResponseID                 *string
	PendingPrewarmID               *string
	LastResponsePendingToolCallIDs *[]string
	PinnedAuthID                   *string
	PinnedAuthByProvider           map[string]responsesWebsocketPinnedAuthState
	PassthroughModelName           *string
	UpstreamMode                   *string
	UpstreamWebsocketAuthID        *string
}

type responsesWebsocketHandoffCompaction struct {
	ModelName   string `json:"model_name,omitempty"`
	ModelKey    string `json:"model_key,omitempty"`
	AuthID      string `json:"auth_id,omitempty"`
	PluginID    string `json:"plugin_id,omitempty"`
	Provider    string `json:"provider,omitempty"`
	TargetModel string `json:"target_model,omitempty"`
}

type responsesWebsocketHandoffPin struct {
	AuthID   string `json:"auth_id"`
	ModelKey string `json:"model_key"`
}

// responsesWebsocketHandoffState is the session part of conn.ws.responses.v1.
type responsesWebsocketHandoffState struct {
	LastRequest                    []byte                                  `json:"last_request,omitempty"`
	LastResponseOutput             []byte                                  `json:"last_response_output,omitempty"`
	ObservedCompaction             responsesWebsocketHandoffCompaction     `json:"observed_compaction"`
	LastResponseID                 string                                  `json:"last_response_id,omitempty"`
	PendingPrewarmID               string                                  `json:"pending_prewarm_id,omitempty"`
	LastResponsePendingToolCallIDs []string                                `json:"last_response_pending_tool_call_ids,omitempty"`
	PinnedAuthID                   string                                  `json:"pinned_auth_id,omitempty"`
	PinnedAuthByProvider           map[string]responsesWebsocketHandoffPin `json:"pinned_auth_by_provider,omitempty"`
	PassthroughModelName           string                                  `json:"passthrough_model_name,omitempty"`
	UpstreamMode                   string                                  `json:"upstream_mode,omitempty"`
	UpstreamWebsocketAuthID        string                                  `json:"upstream_websocket_auth_id,omitempty"`
}

type responsesWebsocketHandoffToolEntry struct {
	CallID string          `json:"call_id"`
	Item   json.RawMessage `json:"item"`
}

// responsesWebsocketHandoffItem is the payload of conn.ws.responses.v1. The
// first file is the client socket, the second (if Upstream is set) the
// upstream plaintext stream.
type responsesWebsocketHandoffItem struct {
	Method        string                               `json:"method"`
	RequestURI    string                               `json:"request_uri"`
	Host          string                               `json:"host"`
	Header        http.Header                          `json:"header"`
	RemoteAddr    string                               `json:"remote_addr"`
	SessionID     string                               `json:"session_id"`
	SessionKey    string                               `json:"session_key"`
	State         responsesWebsocketHandoffState       `json:"state"`
	ToolOutputs   []responsesWebsocketHandoffToolEntry `json:"tool_outputs,omitempty"`
	ToolCalls     []responsesWebsocketHandoffToolEntry `json:"tool_calls,omitempty"`
	ClientCarry   []byte                               `json:"client_carry,omitempty"`
	Upstream      *handoff.UpstreamState               `json:"upstream,omitempty"`
	UpstreamCarry []byte                               `json:"upstream_carry,omitempty"`
}

type responsesWebsocketSeedKey struct{}

func responsesWebsocketSeed(ctx context.Context) *responsesWebsocketHandoffItem {
	if ctx == nil {
		return nil
	}
	seed, _ := ctx.Value(responsesWebsocketSeedKey{}).(*responsesWebsocketHandoffItem)
	return seed
}

// responsesWebsocketSessionID keeps an adopted session's execution session id,
// so its upstream websocket and disconnect subscription carry over; a new
// session gets fresh.
func responsesWebsocketSessionID(c *gin.Context, fresh string) string {
	if c != nil && c.Request != nil {
		if seed := responsesWebsocketSeed(c.Request.Context()); seed != nil && seed.SessionID != "" {
			return seed.SessionID
		}
	}
	return fresh
}

// bindResponsesWebsocketHandoff restores an adopted session's variables and
// registers the session for handoff. It returns the input channel the loop
// must read and the park function it calls before waiting for a message.
// Sessions with response steering read input during a turn and stay put.
func (h *OpenAIResponsesAPIHandler) bindResponsesWebsocketHandoff(c *gin.Context, input <-chan cliproxyexecutor.WebsocketInput, steering bool, vars *responsesWebsocketHandoffVars) (<-chan cliproxyexecutor.WebsocketInput, func()) {
	ctx := c.Request.Context()
	if seed := responsesWebsocketSeed(ctx); seed != nil {
		vars.restore(seed.State)
	}
	client := handoff.ConnFromContext(ctx)
	if !handoff.Enabled() || steering || client == nil {
		return input, func() {}
	}
	session := &responsesWebsocketHandoffSession{
		vars:     vars,
		request:  c.Request,
		client:   client,
		original: input,
		out:      make(chan cliproxyexecutor.WebsocketInput),
		wake:     make(chan struct{}, 1),
	}
	session.cond = sync.NewCond(&session.mu)
	go session.forward(ctx)
	session.unregister = handoff.RegisterSession(session)
	go func() {
		<-ctx.Done()
		session.unregister()
	}()
	return session.out, session.park
}

func (v *responsesWebsocketHandoffVars) snapshot() responsesWebsocketHandoffState {
	state := responsesWebsocketHandoffState{
		LastRequest:                    append([]byte(nil), (*v.LastRequest)...),
		LastResponseOutput:             append([]byte(nil), (*v.LastResponseOutput)...),
		LastResponseID:                 *v.LastResponseID,
		PendingPrewarmID:               *v.PendingPrewarmID,
		LastResponsePendingToolCallIDs: append([]string(nil), (*v.LastResponsePendingToolCallIDs)...),
		PinnedAuthID:                   *v.PinnedAuthID,
		PassthroughModelName:           *v.PassthroughModelName,
		UpstreamMode:                   *v.UpstreamMode,
		UpstreamWebsocketAuthID:        *v.UpstreamWebsocketAuthID,
	}
	compaction := v.ObservedCompaction
	state.ObservedCompaction = responsesWebsocketHandoffCompaction{
		ModelName: compaction.modelName, ModelKey: compaction.modelKey, AuthID: compaction.authID,
		PluginID: compaction.pluginID, Provider: compaction.provider, TargetModel: compaction.targetModel,
	}
	if len(v.PinnedAuthByProvider) > 0 {
		state.PinnedAuthByProvider = make(map[string]responsesWebsocketHandoffPin, len(v.PinnedAuthByProvider))
		for provider, pin := range v.PinnedAuthByProvider {
			state.PinnedAuthByProvider[provider] = responsesWebsocketHandoffPin{AuthID: pin.authID, ModelKey: pin.modelKey}
		}
	}
	return state
}

func (v *responsesWebsocketHandoffVars) restore(state responsesWebsocketHandoffState) {
	*v.LastRequest = state.LastRequest
	if state.LastResponseOutput != nil {
		*v.LastResponseOutput = state.LastResponseOutput
	}
	*v.LastResponseID = state.LastResponseID
	*v.PendingPrewarmID = state.PendingPrewarmID
	*v.LastResponsePendingToolCallIDs = state.LastResponsePendingToolCallIDs
	*v.PinnedAuthID = state.PinnedAuthID
	*v.PassthroughModelName = state.PassthroughModelName
	*v.UpstreamMode = state.UpstreamMode
	*v.UpstreamWebsocketAuthID = state.UpstreamWebsocketAuthID
	compaction := state.ObservedCompaction
	*v.ObservedCompaction = responsesWebsocketObservedCompactionState{
		modelName: compaction.ModelName, modelKey: compaction.ModelKey, authID: compaction.AuthID,
		pluginID: compaction.PluginID, provider: compaction.Provider, targetModel: compaction.TargetModel,
	}
	for provider, pin := range state.PinnedAuthByProvider {
		v.PinnedAuthByProvider[provider] = responsesWebsocketPinnedAuthState{authID: pin.AuthID, modelKey: pin.ModelKey}
	}
}

// responsesWebsocketHandoffSession sits between the socket reader and the
// handler loop. The loop reads the session's out channel; the forwarder can be
// quiesced so that, while the controller exports the session, no message can
// be on its way from the socket to the loop.
type responsesWebsocketHandoffSession struct {
	vars       *responsesWebsocketHandoffVars
	request    *http.Request
	client     *handoff.Conn
	original   <-chan cliproxyexecutor.WebsocketInput
	out        chan cliproxyexecutor.WebsocketInput
	wake       chan struct{}
	unregister func()

	mu        sync.Mutex
	cond      *sync.Cond
	idle      bool // the loop is waiting for a message
	quiesce   bool // the controller asked the forwarder to pause
	quiescent bool // the forwarder is paused holding nothing
	gone      bool // handed off
}

// park runs in the handler loop right before it waits. The mutex orders the
// loop's last writes to its locals before the controller's reads.
func (s *responsesWebsocketHandoffSession) park() {
	s.mu.Lock()
	s.idle = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *responsesWebsocketHandoffSession) forward(ctx context.Context) {
	defer close(s.out)
	for {
		s.mu.Lock()
		for s.quiesce {
			s.quiescent = true
			s.cond.Broadcast()
			s.cond.Wait()
		}
		s.quiescent = false
		s.mu.Unlock()
		select {
		case message, ok := <-s.original:
			if !ok {
				return
			}
			s.mu.Lock()
			s.idle = false
			s.mu.Unlock()
			select {
			case s.out <- message:
			case <-ctx.Done():
				return
			}
		case <-s.wake:
		case <-ctx.Done():
			return
		}
	}
}

// TryHandoff implements handoff.Session.
func (s *responsesWebsocketHandoffSession) TryHandoff() bool {
	s.mu.Lock()
	if s.gone {
		s.mu.Unlock()
		return true
	}
	s.quiesce = true
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	defer func() {
		s.mu.Lock()
		s.quiesce = false
		s.cond.Broadcast()
		s.mu.Unlock()
	}()

	s.mu.Lock()
	deadline := time.Now().Add(100 * time.Millisecond)
	for !(s.quiescent && s.idle) && time.Now().Before(deadline) {
		s.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		s.mu.Lock()
	}
	ready := s.quiescent && s.idle && len(s.original) == 0
	s.mu.Unlock()
	if !ready {
		return false
	}

	// The loop is waiting, the forwarder holds nothing. Stop the client
	// reader at a frame boundary, and the upstream reader if there is one.
	if !s.client.Hold() {
		return false
	}
	upstream, upstreamState, hasUpstream := handoff.ExportUpstream(*s.vars.SessionID)
	if hasUpstream && !upstream.Hold() {
		s.client.Release()
		return false
	}
	if len(s.original) != 0 {
		// A message slipped in before the hold.
		s.client.Release()
		if hasUpstream {
			upstream.Release()
		}
		return false
	}

	item := responsesWebsocketHandoffItem{
		Method:     s.request.Method,
		RequestURI: s.request.RequestURI,
		Host:       s.request.Host,
		Header:     s.request.Header.Clone(),
		RemoteAddr: s.request.RemoteAddr,
		SessionID:  *s.vars.SessionID,
		SessionKey: *s.vars.SessionKey,
		State:      s.vars.snapshot(),
	}
	item.ToolOutputs = exportWebsocketToolCache(defaultWebsocketToolOutputCache, item.SessionKey)
	item.ToolCalls = exportWebsocketToolCache(defaultWebsocketToolCallCache, item.SessionKey)

	s.client.Commit()
	if hasUpstream {
		upstream.Commit()
	}
	s.mu.Lock()
	s.gone = true
	s.mu.Unlock()

	clientFile, clientCarry, errClient := s.client.Extract()
	if errClient != nil {
		log.Errorf("handoff: responses websocket %s: client socket: %v", item.SessionID, errClient)
		return true
	}
	item.ClientCarry = clientCarry
	files := []*os.File{clientFile}
	if hasUpstream {
		upstreamFile, upstreamCarry, errUpstream := handoff.Transfer(upstream)
		if errUpstream != nil {
			log.Errorf("handoff: responses websocket %s: upstream: %v", item.SessionID, errUpstream)
		} else {
			item.Upstream = &upstreamState
			item.UpstreamCarry = upstreamCarry
			files = append(files, upstreamFile)
		}
	}
	sink := handoff.CurrentSink()
	if sink == nil {
		log.Errorf("handoff: responses websocket %s: no new process to send to", item.SessionID)
		return true
	}
	if errSend := sink.Send(sdkhandoff.KindConnWSResponses, item.SessionID, item, files...); errSend != nil {
		log.Errorf("handoff: responses websocket %s: %v", item.SessionID, errSend)
	} else {
		log.Infof("handoff: responses websocket %s handed to the new process", item.SessionID)
	}
	return true
}

// AdoptResponsesWebsocket rebuilds a session received from the old process.
// It returns the client Conn and the upgrade request to replay through the
// normal handler chain; the request carries the session's state.
func AdoptResponsesWebsocket(ctx context.Context, msg sdkhandoff.Message, files []*os.File) (*handoff.Conn, *http.Request, error) {
	closeAll := func() {
		for _, file := range files {
			_ = file.Close()
		}
	}
	var item responsesWebsocketHandoffItem
	if errDecode := msg.Decode(&item); errDecode != nil {
		closeAll()
		return nil, nil, errDecode
	}
	if len(files) == 0 {
		return nil, nil, errors.New("handoff: responses websocket item without a socket")
	}
	if item.Upstream != nil {
		if len(files) < 2 {
			closeAll()
			return nil, nil, errors.New("handoff: responses websocket item without its upstream socket")
		}
		if errUpstream := handoff.AdoptUpstream(ctx, *item.Upstream, files[1], item.UpstreamCarry); errUpstream != nil {
			// The session continues; its next turn dials a fresh upstream.
			log.Warnf("handoff: responses websocket %s: upstream not adopted: %v", item.SessionID, errUpstream)
		}
	}
	for _, extra := range files[min(len(files), 2):] {
		_ = extra.Close()
	}
	importWebsocketToolCache(defaultWebsocketToolOutputCache, item.SessionKey, item.ToolOutputs)
	importWebsocketToolCache(defaultWebsocketToolCallCache, item.SessionKey, item.ToolCalls)

	conn, errConn := handoff.AdoptServerWebsocket(files[0], item.ClientCarry)
	if errConn != nil {
		return nil, nil, errConn
	}
	requestURL, errURL := url.ParseRequestURI(item.RequestURI)
	if errURL != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("handoff: responses websocket request uri: %w", errURL)
	}
	reqCtx := context.WithValue(handoff.ContextWithConn(ctx, conn), responsesWebsocketSeedKey{}, &item)
	req := (&http.Request{
		Method:     item.Method,
		URL:        requestURL,
		RequestURI: item.RequestURI,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     item.Header,
		Host:       item.Host,
		RemoteAddr: item.RemoteAddr,
		Body:       http.NoBody,
	}).WithContext(reqCtx)
	return conn, req, nil
}

func exportWebsocketToolCache(cache *websocketToolOutputCache, sessionKey string) []responsesWebsocketHandoffToolEntry {
	if cache == nil || sessionKey == "" {
		return nil
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	session := cache.sessions[sessionKey]
	if session == nil {
		return nil
	}
	entries := make([]responsesWebsocketHandoffToolEntry, 0, len(session.order))
	for _, callID := range session.order {
		if item, ok := session.outputs[callID]; ok {
			entries = append(entries, responsesWebsocketHandoffToolEntry{CallID: callID, Item: append(json.RawMessage(nil), item...)})
		}
	}
	return entries
}

func importWebsocketToolCache(cache *websocketToolOutputCache, sessionKey string, entries []responsesWebsocketHandoffToolEntry) {
	for _, entry := range entries {
		cache.record(sessionKey, entry.CallID, entry.Item)
	}
}
