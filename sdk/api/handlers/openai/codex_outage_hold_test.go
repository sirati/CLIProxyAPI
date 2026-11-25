package openai

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type codexHoldStatusError struct {
	status int
	msg    string
}

func (e codexHoldStatusError) Error() string   { return e.msg }
func (e codexHoldStatusError) StatusCode() int { return e.status }

// codexHoldExecutor fails its first failures stream calls with an overload.
type codexHoldExecutor struct {
	id       string
	failures int
	mu       sync.Mutex
	calls    int
}

func (e *codexHoldExecutor) Identifier() string { return e.id }

func (e *codexHoldExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *codexHoldExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	n := e.calls
	e.mu.Unlock()
	if n <= e.failures {
		return nil, codexHoldStatusError{http.StatusServiceUnavailable, `{"error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`}
	}
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-hold\",\"output\":[]}}\n\n")}
	chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-hold\",\"output\":[]}}\n\n")}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *codexHoldExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (*codexHoldExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*codexHoldExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*codexHoldExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func newCodexHoldRouter(t *testing.T, executor *codexHoldExecutor) (*gin.Engine, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	restore := coreauth.SetOutageGateTimingForTest(time.Millisecond, 4*time.Millisecond)
	t.Cleanup(restore)
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{DisableCooling: true})
	manager.RegisterExecutor(executor)
	model := executor.id + "-model"
	auth := &coreauth.Auth{ID: executor.id + "-auth", Provider: executor.id, Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	router := gin.New()
	router.POST("/v1/responses", NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)).Responses)
	return router, model
}

func postCodexHoldRequest(router *gin.Engine, model string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"`+model+`","stream":true,"input":"hi"}`))
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

// T3 Code runs Codex under its own name ("T3 Code/0.171.0" in the user agent
// and originator); Codex's own headers still mark it. Its stream waits out an
// overload instead of failing.
func TestCodexClientStreamWaitsOutAnOverload(t *testing.T) {
	executor := &codexHoldExecutor{id: "codex-hold-overload", failures: 3}
	router, model := newCodexHoldRouter(t, executor)

	recorder := postCodexHoldRequest(router, model, map[string]string{
		"User-Agent":        "T3 Code/0.171.0 (NixOS 26.11; x86_64) unknown",
		"Originator":        "T3 Code",
		"X-Codex-Window-Id": "window-1",
	})

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "response.completed") {
		t.Fatalf("status = %d body = %q, want the completed response", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "overloaded") {
		t.Fatalf("the overload reached the client: %q", recorder.Body.String())
	}
	if got := executor.Calls(); got != 4 {
		t.Fatalf("upstream calls = %d, want 4", got)
	}
}

func TestOtherResponsesClientsStillSeeAnOverload(t *testing.T) {
	executor := &codexHoldExecutor{id: "codex-hold-other-client", failures: 3}
	router, model := newCodexHoldRouter(t, executor)

	recorder := postCodexHoldRequest(router, model, map[string]string{"User-Agent": "curl/8.0"})

	if recorder.Code == http.StatusOK || executor.Calls() != 1 {
		t.Fatalf("status = %d after %d calls, want the overload after 1", recorder.Code, executor.Calls())
	}
}

func TestCodexClientIsRecognisedByItsOwnHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, header := range []string{"X-Codex-Installation-Id", "X-Codex-Window-Id", "X-Codex-Turn-Metadata"} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		c.Request.Header.Set("User-Agent", "T3 Code/0.171.0")
		c.Request.Header.Set(header, "x")
		if !isCodexResponsesClientRequest(c) {
			t.Errorf("a request with %s was not recognised as Codex", header)
		}
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "curl/8.0")
	if isCodexResponsesClientRequest(c) {
		t.Error("a plain client was recognised as Codex")
	}
}
