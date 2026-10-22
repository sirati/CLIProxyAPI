// Package workload drives a proxy the way clients do during the handoff
// tests: Claude streams over HTTP/1.1 keep-alive and a Codex Responses
// websocket session with previous_response_id continuity.
package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// ClaudeStream posts a streaming Messages request with text as the prompt and
// returns the body and whether the connection was reused.
func ClaudeStream(client *http.Client, url, key, text string) (string, bool, error) {
	body := fmt.Sprintf(`{"model":"claude-fake","max_tokens":16,"stream":true,"messages":[{"role":"user","content":%q}]}`, text)
	reused := false
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}
	req, errReq := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), http.MethodPost, url, strings.NewReader(body))
	if errReq != nil {
		return "", false, errReq
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", key)
	req.Header.Set("Anthropic-Version", "2023-06-01")
	resp, errDo := client.Do(req)
	if errDo != nil {
		return "", reused, errDo
	}
	defer func() { _ = resp.Body.Close() }()
	data, errRead := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return string(data), reused, fmt.Errorf("status %d: %s", resp.StatusCode, data)
	}
	if errRead == nil && !strings.Contains(string(data), "message_stop") {
		errRead = fmt.Errorf("stream ended early: %q", data)
	}
	return string(data), reused, errRead
}

// CodexSession is a client of the Responses websocket.
type CodexSession struct {
	conn *websocket.Conn
	last string
}

// DialCodex opens a session on wsURL (ws://host/v1/responses).
func DialCodex(wsURL, key string) (*CodexSession, error) {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+key)
	conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, header)
	if errDial != nil {
		return nil, errDial
	}
	return &CodexSession{conn: conn}, nil
}

// Close ends the session.
func (c *CodexSession) Close() error { return c.conn.Close() }

// Turn sends one response.create, continuing the previous response, and
// returns the completed response's text.
func (c *CodexSession) Turn(text string) (string, error) {
	request := map[string]any{
		"type": "response.create", "model": "gpt-fake",
		"input": []any{map[string]any{"type": "message", "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": text}}}},
	}
	if c.last != "" {
		request["previous_response_id"] = c.last
	}
	if errWrite := c.conn.WriteJSON(request); errWrite != nil {
		return "", errWrite
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	for {
		_, payload, errRead := c.conn.ReadMessage()
		if errRead != nil {
			return "", fmt.Errorf("websocket read: %w", errRead)
		}
		var event struct {
			Type     string `json:"type"`
			Response struct {
				ID     string `json:"id"`
				Output []struct {
					Content []struct {
						Text string `json:"text"`
					} `json:"content"`
				} `json:"output"`
			} `json:"response"`
		}
		_ = json.Unmarshal(payload, &event)
		switch event.Type {
		case "response.completed":
			c.last = event.Response.ID
			if len(event.Response.Output) > 0 && len(event.Response.Output[0].Content) > 0 {
				return event.Response.Output[0].Content[0].Text, nil
			}
			return "", nil
		case "error", "response.failed":
			return "", fmt.Errorf("turn failed: %s", payload)
		}
	}
}
