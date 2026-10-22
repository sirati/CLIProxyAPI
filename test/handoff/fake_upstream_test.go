package handoff_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// fakeClaude is an Anthropic Messages upstream. A request whose text holds
// "stream:N" sends N deltas 100ms apart. "fail-first" makes the first key
// that sees it wait and answer 429, so the proxy has to choose another
// credential; that key keeps failing such requests.
type fakeClaude struct {
	server   *httptest.Server
	requests atomic.Int64
	mu       sync.Mutex
	failKey  string // the key that got the first "fail-first" request
}

func (f *fakeClaude) failedKey() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failKey
}

func newFakeClaude() *fakeClaude {
	f := &fakeClaude{}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	return f
}

func (f *fakeClaude) serve(w http.ResponseWriter, r *http.Request) {
	f.requests.Add(1)
	body, _ := io.ReadAll(r.Body)
	text := string(body)
	key := r.Header.Get("X-Api-Key")
	if key == "" {
		key = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	fail := false
	if strings.Contains(text, "fail-first") {
		f.mu.Lock()
		if f.failKey == "" {
			f.failKey = key
		}
		fail = f.failKey == key
		f.mu.Unlock()
	}
	if fail {
		time.Sleep(1500 * time.Millisecond)
		w.Header().Set("Retry-After", "60")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`))
		return
	}
	deltas := 3
	if idx := strings.Index(text, "stream:"); idx >= 0 {
		digits := text[idx+len("stream:"):]
		end := strings.IndexFunc(digits, func(r rune) bool { return r < '0' || r > '9' })
		if end > 0 {
			deltas, _ = strconv.Atoi(digits[:end])
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	flusher := w.(http.Flusher)
	send := func(event string, data string) {
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		flusher.Flush()
	}
	send("message_start", `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-fake","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":0}}}`)
	send("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
	for i := 0; i < deltas; i++ {
		send("content_block_delta", fmt.Sprintf(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"[%d|%s]"}}`, i, key))
		if deltas > 3 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	send("content_block_stop", `{"type":"content_block_stop","index":0}`)
	send("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}`)
	send("message_stop", `{"type":"message_stop"}`)
}

// fakeCodex is a Responses websocket upstream over TLS. Like the real one it
// only knows previous_response_id values from the same connection; every
// reply names the connection, so a test sees whether it changed.
type fakeCodex struct {
	server      *httptest.Server
	connections atomic.Int64
	mu          sync.Mutex
	errors      []string
}

func newFakeCodex() *fakeCodex {
	f := &fakeCodex{}
	f.server = httptest.NewTLSServer(http.HandlerFunc(f.serve))
	return f
}

func (f *fakeCodex) serve(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{EnableCompression: true}
	conn, errUpgrade := upgrader.Upgrade(w, r, nil)
	if errUpgrade != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	connID := f.connections.Add(1)
	turn := 0
	lastID := ""
	for {
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			return
		}
		var request map[string]any
		if json.Unmarshal(payload, &request) != nil {
			continue
		}
		if request["type"] != "response.create" {
			continue
		}
		if previous, _ := request["previous_response_id"].(string); previous != "" && previous != lastID {
			f.mu.Lock()
			f.errors = append(f.errors, fmt.Sprintf("conn%d: unknown previous_response_id %s", connID, previous))
			f.mu.Unlock()
			_ = conn.WriteJSON(map[string]any{"type": "error", "status": 400, "error": map[string]any{
				"type": "invalid_request_error", "code": "previous_response_not_found",
				"message": "Previous response not found.",
			}})
			continue
		}
		turn++
		id := fmt.Sprintf("resp_c%d_t%d", connID, turn)
		text := fmt.Sprintf("conn=%d turn=%d", connID, turn)
		item := map[string]any{"id": "msg_" + id, "type": "message", "role": "assistant", "status": "completed",
			"content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}}
		response := func(status string, output []any) map[string]any {
			return map[string]any{"id": id, "object": "response", "status": status, "model": "gpt-fake",
				"output": output, "usage": map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}
		}
		_ = conn.WriteJSON(map[string]any{"type": "response.created", "sequence_number": 0, "response": response("in_progress", []any{})})
		_ = conn.WriteJSON(map[string]any{"type": "response.output_item.added", "sequence_number": 1, "output_index": 0, "item": item})
		_ = conn.WriteJSON(map[string]any{"type": "response.output_text.delta", "sequence_number": 2, "output_index": 0, "content_index": 0, "item_id": "msg_" + id, "delta": text})
		_ = conn.WriteJSON(map[string]any{"type": "response.output_item.done", "sequence_number": 3, "output_index": 0, "item": item})
		_ = conn.WriteJSON(map[string]any{"type": "response.completed", "sequence_number": 4, "response": response("completed", []any{item})})
		lastID = id
	}
}

func (f *fakeCodex) protocolErrors() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.errors...)
}
