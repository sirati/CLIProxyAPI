package gemini

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/handoff"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

const genaiUserAgent = "google-genai-sdk/1.79.1 gl-go/go1.28"

type genaiTestError struct {
	status     int
	retryAfter time.Duration
	msg        string
}

func (e genaiTestError) Error() string   { return e.msg }
func (e genaiTestError) StatusCode() int { return e.status }
func (e genaiTestError) RetryAfter() *time.Duration {
	if e.retryAfter <= 0 {
		return nil
	}
	d := e.retryAfter
	return &d
}

// scriptedGeminiExecutor answers stream call n (from 1) with script(n).
type scriptedGeminiExecutor struct {
	id     string
	script func(n int) (*coreexecutor.StreamResult, error)
	mu     sync.Mutex
	calls  int
}

func (e *scriptedGeminiExecutor) Identifier() string { return e.id }

func (e *scriptedGeminiExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *scriptedGeminiExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	n := e.calls
	e.mu.Unlock()
	return e.script(n)
}

func (e *scriptedGeminiExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (*scriptedGeminiExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*scriptedGeminiExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*scriptedGeminiExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func streamOf(items ...coreexecutor.StreamChunk) *coreexecutor.StreamResult {
	chunks := make(chan coreexecutor.StreamChunk, len(items))
	for _, item := range items {
		chunks <- item
	}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}
}

const genaiTestChunk = `{"candidates":[{"content":{"role":"model","parts":[{"text":"pong"}]},"finishReason":"STOP","index":0}]}`

// fastGenAIHold shortens the hold's waits for a test.
func fastGenAIHold(t *testing.T, headerDeadline time.Duration) {
	t.Helper()
	deadline, first, maxWait := genaiHeaderDeadline, genaiFirstRetryWait, genaiMaxRetryWait
	genaiHeaderDeadline, genaiFirstRetryWait, genaiMaxRetryWait = headerDeadline, time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() {
		genaiHeaderDeadline, genaiFirstRetryWait, genaiMaxRetryWait = deadline, first, maxWait
	})
}

func newGenAITestRouter(t *testing.T, executor *scriptedGeminiExecutor, cooling bool) (*gin.Engine, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{DisableCooling: !cooling})
	manager.RegisterExecutor(executor)
	model := executor.id + "-model"
	auth := &coreauth.Auth{ID: executor.id + "-auth", Provider: executor.id, Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

	h := NewGeminiAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	router := gin.New()
	router.POST("/v1beta/models/*action", h.GeminiHandler)
	return router, model
}

func serveGenAI(router *gin.Engine, model, userAgent string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1beta/models/"+model+":streamGenerateContent?alt=sse", strings.NewReader(`{"contents":[{"parts":[{"text":"hi"}]}]}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", userAgent)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestGenAIStreamHoldsTransientFailuresUntilTheUpstreamAnswers(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	failures := []error{
		genaiTestError{status: http.StatusServiceUnavailable, msg: "No capacity available for model on the server"},
		genaiTestError{status: http.StatusInternalServerError, msg: "internal error"},
		errors.New(`Post "https://daily-cloudcode-pa.googleapis.com/v1internal": connection reset by peer`),
	}
	executor := &scriptedGeminiExecutor{id: "genai-hold-transient", script: func(n int) (*coreexecutor.StreamResult, error) {
		if n <= len(failures) {
			return nil, failures[n-1]
		}
		return streamOf(coreexecutor.StreamChunk{Payload: []byte(genaiTestChunk)}), nil
	}}
	router, model := newGenAITestRouter(t, executor, false)

	recorder := serveGenAI(router, model, genaiUserAgent)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", recorder.Code, recorder.Body.String())
	}
	if want := "data: " + genaiTestChunk + "\n\n"; recorder.Body.String() != want {
		t.Fatalf("body = %q, want only the answer %q", recorder.Body.String(), want)
	}
	if got := executor.Calls(); got != len(failures)+1 {
		t.Fatalf("upstream calls = %d, want %d", got, len(failures)+1)
	}
}

func TestGenAIStreamWaitsOutACredentialCooldown(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	executor := &scriptedGeminiExecutor{id: "genai-hold-cooldown", script: func(n int) (*coreexecutor.StreamResult, error) {
		if n == 1 {
			return nil, genaiTestError{status: http.StatusServiceUnavailable, retryAfter: 50 * time.Millisecond, msg: "No capacity available for model on the server"}
		}
		return streamOf(coreexecutor.StreamChunk{Payload: []byte(genaiTestChunk)}), nil
	}}
	router, model := newGenAITestRouter(t, executor, true)

	recorder := serveGenAI(router, model, genaiUserAgent)

	if recorder.Code != http.StatusOK || !strings.HasPrefix(recorder.Body.String(), "data: ") {
		t.Fatalf("status = %d body = %q, want the answer after the cooldown", recorder.Code, recorder.Body.String())
	}
	if got := executor.Calls(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2: retries during the cooldown must not reach the upstream", got)
	}
}

func TestGenAIStreamReturnsAPermanentErrorAtOnce(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	executor := &scriptedGeminiExecutor{id: "genai-hold-permanent", script: func(int) (*coreexecutor.StreamResult, error) {
		return nil, genaiTestError{status: http.StatusBadRequest, msg: "request contains an invalid argument"}
	}}
	router, model := newGenAITestRouter(t, executor, false)

	recorder := serveGenAI(router, model, genaiUserAgent)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%q", recorder.Code, recorder.Body.String())
	}
	if got := executor.Calls(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
}

func TestGenAIStreamReportsAPermanentErrorAfterHoldingAsABareErrorLine(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	executor := &scriptedGeminiExecutor{id: "genai-hold-late-permanent", script: func(n int) (*coreexecutor.StreamResult, error) {
		if n == 1 {
			return nil, genaiTestError{status: http.StatusServiceUnavailable, msg: "unavailable"}
		}
		return nil, genaiTestError{status: http.StatusBadRequest, msg: "request contains an invalid argument"}
	}}
	router, model := newGenAITestRouter(t, executor, false)

	recorder := serveGenAI(router, model, genaiUserAgent)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 once the hold sent headers; body=%q", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.HasPrefix(body, `{"error":{"code":400,`) || !strings.Contains(body, `"status":"INVALID_ARGUMENT"`) || !strings.HasSuffix(body, "}\n\n") {
		t.Fatalf("body = %q, want one bare genai error line", body)
	}
}

func TestGenAIStreamReportsAMidStreamFailureAsARetryableErrorLine(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	executor := &scriptedGeminiExecutor{id: "genai-hold-midstream", script: func(int) (*coreexecutor.StreamResult, error) {
		return streamOf(
			coreexecutor.StreamChunk{Payload: []byte(genaiTestChunk)},
			coreexecutor.StreamChunk{Err: errors.New("unexpected EOF")},
		), nil
	}}
	router, model := newGenAITestRouter(t, executor, false)

	recorder := serveGenAI(router, model, genaiUserAgent)

	body := recorder.Body.String()
	want := "data: " + genaiTestChunk + "\n\n" + `{"error":{"code":503,"message":"unexpected EOF","status":"UNAVAILABLE"}}` + "\n\n"
	if body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

func TestGenAIStreamLeavesOtherClientsAlone(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	executor := &scriptedGeminiExecutor{id: "genai-hold-other-client", script: func(int) (*coreexecutor.StreamResult, error) {
		return nil, genaiTestError{status: http.StatusServiceUnavailable, msg: "unavailable"}
	}}
	router, model := newGenAITestRouter(t, executor, false)

	recorder := serveGenAI(router, model, "curl/8.0")

	if recorder.Code != http.StatusServiceUnavailable || executor.Calls() != 1 {
		t.Fatalf("status = %d after %d calls, want one 503", recorder.Code, executor.Calls())
	}
}

// The Antigravity CLI gives up on a response whose headers take longer than
// 60 seconds. A slow upstream must not run into that.
func TestGenAIStreamSendsHeadersBeforeTheClientsHeaderTimeout(t *testing.T) {
	fastGenAIHold(t, 20*time.Millisecond)
	release := make(chan struct{})
	executor := &scriptedGeminiExecutor{id: "genai-hold-slow", script: func(int) (*coreexecutor.StreamResult, error) {
		chunks := make(chan coreexecutor.StreamChunk)
		go func() {
			defer close(chunks)
			<-release
			chunks <- coreexecutor.StreamChunk{Payload: []byte(genaiTestChunk)}
		}()
		return &coreexecutor.StreamResult{Chunks: chunks}, nil
	}}
	router, model := newGenAITestRouter(t, executor, false)
	server := httptest.NewServer(router)
	defer server.Close()

	client := &http.Client{Transport: &http.Transport{ResponseHeaderTimeout: 2 * time.Second}}
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1beta/models/"+model+":streamGenerateContent?alt=sse", strings.NewReader(`{"contents":[{"parts":[{"text":"hi"}]}]}`))
	request.Header.Set("User-Agent", genaiUserAgent)
	response, errDo := client.Do(request)
	if errDo != nil {
		t.Fatalf("request failed before headers: %v", errDo)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status = %d content-type = %q, want an SSE stream", response.StatusCode, response.Header.Get("Content-Type"))
	}
	close(release)
	body, errRead := io.ReadAll(response.Body)
	if errRead != nil {
		t.Fatalf("read body: %v", errRead)
	}
	if want := "data: " + genaiTestChunk + "\n\n"; string(body) != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

// A process that handed its work to a newer one must end a held stream with a
// retryable error, so the client's retry reaches the new process.
func TestGenAIStreamEndsAHeldStreamAfterAHandoff(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	t.Cleanup(handoff.Unfreeze)
	executor := &scriptedGeminiExecutor{id: "genai-hold-handoff", script: func(n int) (*coreexecutor.StreamResult, error) {
		if n == 2 {
			handoff.Freeze(nil)
		}
		return nil, genaiTestError{status: http.StatusServiceUnavailable, msg: "No capacity available for model on the server"}
	}}
	router, model := newGenAITestRouter(t, executor, false)

	recorder := serveGenAI(router, model, genaiUserAgent)

	want := `{"error":{"code":503,"message":"No capacity available for model on the server","status":"UNAVAILABLE"}}` + "\n\n"
	if recorder.Code != http.StatusOK || recorder.Body.String() != want {
		t.Fatalf("status = %d body = %q, want 200 and %q", recorder.Code, recorder.Body.String(), want)
	}
	if got := executor.Calls(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2", got)
	}
}
