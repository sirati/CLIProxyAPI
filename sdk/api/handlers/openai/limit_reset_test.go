package openai

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	"github.com/tidwall/gjson"
)

func codexCooldownMessage() *interfaces.ErrorMessage {
	return &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      coreauth.NewModelCooldownError("gpt-6.1-sol", "codex", 90*time.Minute),
	}
}

func TestResponsesWriteErrorResponseAnswersAUsageLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))

	h.WriteErrorResponse(c, codexCooldownMessage())

	body := recorder.Body.Bytes()
	if recorder.Code != http.StatusTooManyRequests || gjson.GetBytes(body, "error.type").String() != "usage_limit_reached" {
		t.Fatalf("status = %d, body = %s", recorder.Code, body)
	}
	if got := gjson.GetBytes(body, "error.resets_in_seconds").Int(); got != 5400 {
		t.Fatalf("resets_in_seconds = %d, want 5400", got)
	}
	resetsAt := gjson.GetBytes(body, "error.resets_at").String()
	if recorder.Header().Get("X-Codex-Primary-Used-Percent") != "100" || recorder.Header().Get("X-Codex-Primary-Reset-At") != resetsAt {
		t.Fatalf("headers = %v, want a full primary window resetting at %s", recorder.Header(), resetsAt)
	}
}

func TestResponsesWriteErrorResponseKeepsOtherErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))

	h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New("upstream broke")})

	if recorder.Code != http.StatusBadGateway || recorder.Header().Get("X-Codex-Primary-Used-Percent") != "" {
		t.Fatalf("status = %d, headers = %v", recorder.Code, recorder.Header())
	}
}

func TestResponsesWebsocketAnswersAUsageLimit(t *testing.T) {
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	serverErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		_, _, errWrite := h.writeResponsesWebsocketTerminalError(newResponsesWebsocketWriter(conn), nil, codexCooldownMessage(), nil)
		if !errors.Is(errWrite, websocket.ErrCloseSent) {
			serverErrCh <- errWrite
			return
		}
		serverErrCh <- nil
	}))
	defer server.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket message: %v", err)
	}
	if errServer := <-serverErrCh; errServer != nil {
		t.Fatalf("server: %v", errServer)
	}

	// Codex reads this event as a 429 response: the error is its body, the
	// headers its rate-limit snapshot.
	if gjson.GetBytes(payload, "type").String() != "error" || gjson.GetBytes(payload, "status").Int() != http.StatusTooManyRequests {
		t.Fatalf("payload = %s", payload)
	}
	if gjson.GetBytes(payload, "error.type").String() != "usage_limit_reached" {
		t.Fatalf("payload = %s", payload)
	}
	resetsAt := strconv.FormatInt(gjson.GetBytes(payload, "error.resets_at").Int(), 10)
	if gjson.GetBytes(payload, "headers.X-Codex-Primary-Used-Percent").String() != "100" ||
		gjson.GetBytes(payload, "headers.X-Codex-Primary-Reset-At").String() != resetsAt {
		t.Fatalf("payload = %s, want a full primary window resetting at %s", payload, resetsAt)
	}
}

func TestResponsesWriteErrorResponseAnswersAPacedRequestWithItsTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))

	h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, Error: coreauth.NewCodexPacedError(83500*time.Millisecond, false)})

	// A plain 429 with Retry-After: Codex sleeps until then and retries.
	body := recorder.Body.Bytes()
	if recorder.Code != http.StatusTooManyRequests || recorder.Header().Get("Retry-After") != "84" {
		t.Fatalf("status = %d, Retry-After = %q, body = %s", recorder.Code, recorder.Header().Get("Retry-After"), body)
	}
	if gjson.GetBytes(body, "error.type").String() == "usage_limit_reached" || recorder.Header().Get("X-Codex-Primary-Used-Percent") != "" {
		t.Fatalf("a paced request was answered as a usage limit: %s", body)
	}
}

func TestResponsesWriteErrorResponseTellsAnExhaustedRetryTheReset(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))

	h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, Error: coreauth.NewCodexPacedError(3*time.Minute, true)})

	body := recorder.Body.Bytes()
	if gjson.GetBytes(body, "error.type").String() != "usage_limit_reached" || gjson.GetBytes(body, "error.resets_in_seconds").Int() != 180 {
		t.Fatalf("status = %d, body = %s; want a usage limit resetting in 180s", recorder.Code, body)
	}
}
