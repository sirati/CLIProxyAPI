package gemini

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
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

var capacityError = genaiTestError{status: http.StatusServiceUnavailable, msg: "No capacity available for model on the server (MODEL_CAPACITY_EXHAUSTED)"}

// scriptedGeminiExecutor answers stream call n (from 1) with script(n, payload).
type scriptedGeminiExecutor struct {
	id       string
	script   func(n int) (*coreexecutor.StreamResult, error)
	mu       sync.Mutex
	calls    int
	payloads [][]byte
}

func (e *scriptedGeminiExecutor) Identifier() string { return e.id }

func (e *scriptedGeminiExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *scriptedGeminiExecutor) ExecuteStream(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	n := e.calls
	e.payloads = append(e.payloads, append([]byte(nil), req.Payload...))
	e.mu.Unlock()
	return e.script(n)
}

func (e *scriptedGeminiExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (e *scriptedGeminiExecutor) Payload(n int) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.payloads[n-1]
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

func textChunk(text string) []byte {
	doc := ojson.NewDoc([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":""}]},"index":0}]}`))
	_ = doc.Set("candidates.0.content.parts.0.text", text)
	return doc.Bytes()
}

func sse(chunks ...[]byte) string {
	var out strings.Builder
	for _, chunk := range chunks {
		out.WriteString("data: ")
		out.Write(chunk)
		out.WriteString("\n\n")
	}
	return out.String()
}

// fastGenAIHold shortens the hold's and the outage gate's waits for a test.
func fastGenAIHold(t *testing.T, headerDeadline time.Duration) {
	t.Helper()
	deadline, first, maxWait := genaiHeaderDeadline, genaiFirstRetryWait, genaiMaxRetryWait
	genaiHeaderDeadline, genaiFirstRetryWait, genaiMaxRetryWait = headerDeadline, time.Millisecond, 20*time.Millisecond
	restoreGate := coreauth.SetOutageGateTimingForTest(time.Millisecond, 4*time.Millisecond)
	t.Cleanup(func() {
		genaiHeaderDeadline, genaiFirstRetryWait, genaiMaxRetryWait = deadline, first, maxWait
		restoreGate()
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

const genaiTestRequest = `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`

func serveGenAI(router *gin.Engine, model, userAgent string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1beta/models/"+model+":streamGenerateContent?alt=sse", strings.NewReader(genaiTestRequest))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", userAgent)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestGenAIStreamWaitsOutAnOutageUntilTheUpstreamAnswers(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	failures := []error{
		capacityError,
		genaiTestError{status: http.StatusInternalServerError, msg: "internal error"},
		&url.Error{Op: "Post", URL: "https://daily-cloudcode-pa.googleapis.com/v1internal", Err: io.ErrUnexpectedEOF},
	}
	executor := &scriptedGeminiExecutor{id: "genai-hold-outage", script: func(n int) (*coreexecutor.StreamResult, error) {
		if n <= len(failures) {
			return nil, failures[n-1]
		}
		return streamOf(coreexecutor.StreamChunk{Payload: []byte(genaiTestChunk)}), nil
	}}
	router, model := newGenAITestRouter(t, executor, true)

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

// While a model is down, the requests waiting for it go out one at a time,
// and when it comes back all of them are answered.
func TestGenAIStreamOutageSendsOneRequestAtATime(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	var down atomic.Int32
	down.Store(12)
	const clients = 4
	var inFlight, maxInFlight atomic.Int32
	allSent := make(chan struct{})
	executor := &scriptedGeminiExecutor{id: "genai-hold-one-at-a-time", script: func(n int) (*coreexecutor.StreamResult, error) {
		// The first calls all go out while the gate is open; every later one
		// is a request the gate let through. Once the model is back the gate
		// opens and lets everything waiting go at once.
		if n <= clients {
			if n == clients {
				close(allSent)
			}
			<-allSent
		} else if down.Load() > 0 {
			now := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				seen := maxInFlight.Load()
				if now <= seen || maxInFlight.CompareAndSwap(seen, now) {
					break
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
		if down.Add(-1) >= 0 {
			return nil, capacityError
		}
		return streamOf(coreexecutor.StreamChunk{Payload: []byte(genaiTestChunk)}), nil
	}}
	router, model := newGenAITestRouter(t, executor, true)

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if recorder := serveGenAI(router, model, genaiUserAgent); recorder.Body.String() != "data: "+genaiTestChunk+"\n\n" {
				t.Errorf("status = %d body = %q, want the answer", recorder.Code, recorder.Body.String())
			}
		}()
	}
	wg.Wait()
	if got := maxInFlight.Load(); got > 1 {
		t.Fatalf("%d requests reached the upstream at once during the outage, want 1", got)
	}
}

// A rate limit the credential pool keeps for itself is retried here.
func TestGenAIStreamRetriesARateLimitUntilItLifts(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	executor := &scriptedGeminiExecutor{id: "genai-hold-rate-limit", script: func(n int) (*coreexecutor.StreamResult, error) {
		if n == 1 {
			return nil, genaiTestError{status: http.StatusTooManyRequests, msg: "rate limited"}
		}
		return streamOf(coreexecutor.StreamChunk{Payload: []byte(genaiTestChunk)}), nil
	}}
	router, model := newGenAITestRouter(t, executor, false)

	recorder := serveGenAI(router, model, genaiUserAgent)

	if recorder.Code != http.StatusOK || recorder.Body.String() != "data: "+genaiTestChunk+"\n\n" {
		t.Fatalf("status = %d body = %q, want the answer", recorder.Code, recorder.Body.String())
	}
	if got := executor.Calls(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2", got)
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

func TestGenAIStreamReportsAPermanentErrorAfterTheHeadersAsABareErrorLine(t *testing.T) {
	fastGenAIHold(t, time.Millisecond)
	release := make(chan struct{})
	executor := &scriptedGeminiExecutor{id: "genai-hold-late-permanent", script: func(int) (*coreexecutor.StreamResult, error) {
		<-release
		return nil, genaiTestError{status: http.StatusBadRequest, msg: "request contains an invalid argument"}
	}}
	router, model := newGenAITestRouter(t, executor, false)
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()

	recorder := serveGenAI(router, model, genaiUserAgent)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 once headers went out; body=%q", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.HasPrefix(body, `{"error":{"code":400,`) || !strings.Contains(body, `"status":"INVALID_ARGUMENT"`) || !strings.HasSuffix(body, "}\n\n") {
		t.Fatalf("body = %q, want one bare genai error line", body)
	}
}

// An answer that breaks off is continued from what the client already has,
// in the same stream.
func TestGenAIStreamContinuesAnAnswerThatBrokeOff(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	first, second := textChunk("Hello, wor"), textChunk("ld!")
	executor := &scriptedGeminiExecutor{id: "genai-hold-continue", script: func(n int) (*coreexecutor.StreamResult, error) {
		if n == 1 {
			return streamOf(coreexecutor.StreamChunk{Payload: first}, coreexecutor.StreamChunk{Err: io.ErrUnexpectedEOF}), nil
		}
		return streamOf(coreexecutor.StreamChunk{Payload: second}), nil
	}}
	router, model := newGenAITestRouter(t, executor, true)

	recorder := serveGenAI(router, model, genaiUserAgent)

	if want := sse(first, second); recorder.Body.String() != want {
		t.Fatalf("body = %q, want the answer joined: %q", recorder.Body.String(), want)
	}
	if got := executor.Calls(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2", got)
	}
	continuation := executor.Payload(2)
	if got := ojson.GetBytes(continuation, "contents.#").Int(); got != 3 {
		t.Fatalf("continuation has %d contents, want the request, the partial answer and the instruction: %s", got, continuation)
	}
	if role, text := ojson.GetBytes(continuation, "contents.1.role").String(), ojson.GetBytes(continuation, "contents.1.parts.0.text").String(); role != "model" || text != "Hello, wor" {
		t.Fatalf("second content has role %q text %q, want the model's partial answer", role, text)
	}
	if role, text := ojson.GetBytes(continuation, "contents.2.role").String(), ojson.GetBytes(continuation, "contents.2.parts.0.text").String(); role != "user" || text != genaiContinueInstruction {
		t.Fatalf("continuation ends with role %q text %q, want the instruction to continue", role, text)
	}
}

// Thoughts are not part of the answer the continuation starts from.
func TestGenAIAnswerKeepsTextAndToolCallsOnly(t *testing.T) {
	var answer genaiAnswer
	answer.add([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"planning","thought":true},{"text":"Hi "}]}}]}`))
	answer.add([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"there"}]}}]}`))
	if answer.text.String() != "Hi there" || answer.toolCall {
		t.Fatalf("text = %q toolCall = %v, want %q and no tool call", answer.text.String(), answer.toolCall, "Hi there")
	}
	answer.add([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"read_file","args":{}}}]}}]}`))
	if !answer.toolCall {
		t.Fatal("a function call part was not seen")
	}
}

// An answer that already holds a tool call ends where it broke off: the
// client runs the tool and goes on.
func TestGenAIStreamEndsAnAnswerWithAToolCallWhereItBrokeOff(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	call := []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"read_file","args":{"Path":"/tmp/x"}}}]},"index":0}]}`)
	executor := &scriptedGeminiExecutor{id: "genai-hold-tool-call", script: func(int) (*coreexecutor.StreamResult, error) {
		return streamOf(coreexecutor.StreamChunk{Payload: call}, coreexecutor.StreamChunk{Err: io.ErrUnexpectedEOF}), nil
	}}
	router, model := newGenAITestRouter(t, executor, true)

	recorder := serveGenAI(router, model, genaiUserAgent)

	if want := sse(call) + string(genaiStopChunk); recorder.Body.String() != want {
		t.Fatalf("body = %q, want %q", recorder.Body.String(), want)
	}
	if got := executor.Calls(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
}

func TestGenAIStreamLeavesOtherClientsAlone(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	executor := &scriptedGeminiExecutor{id: "genai-hold-other-client", script: func(int) (*coreexecutor.StreamResult, error) {
		return nil, capacityError
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
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1beta/models/"+model+":streamGenerateContent?alt=sse", strings.NewReader(genaiTestRequest))
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
	fastGenAIHold(t, time.Millisecond)
	t.Cleanup(handoff.Unfreeze)
	executor := &scriptedGeminiExecutor{id: "genai-hold-handoff", script: func(n int) (*coreexecutor.StreamResult, error) {
		if n == 1 {
			time.Sleep(20 * time.Millisecond) // headers go out meanwhile
		}
		if n == 2 {
			handoff.Freeze(nil)
		}
		return nil, capacityError
	}}
	router, model := newGenAITestRouter(t, executor, false)

	recorder := serveGenAI(router, model, genaiUserAgent)

	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || !strings.HasPrefix(body, `{"error":{"code":503,`) || !strings.Contains(body, `"status":"UNAVAILABLE"`) {
		t.Fatalf("status = %d body = %q, want 200 and one retryable error line", recorder.Code, body)
	}
	if got := executor.Calls(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2", got)
	}
}

// A continuation the upstream refuses ends with a retryable error, so the
// client retries the whole call instead of ending its turn.
func TestGenAIStreamRefusedContinuationIsRetryable(t *testing.T) {
	fastGenAIHold(t, time.Hour)
	first := textChunk("Hello, wor")
	executor := &scriptedGeminiExecutor{id: "genai-hold-refused-continuation", script: func(n int) (*coreexecutor.StreamResult, error) {
		if n == 1 {
			return streamOf(coreexecutor.StreamChunk{Payload: first}, coreexecutor.StreamChunk{Err: io.ErrUnexpectedEOF}), nil
		}
		return nil, genaiTestError{status: http.StatusBadRequest, msg: "request contains an invalid argument"}
	}}
	router, model := newGenAITestRouter(t, executor, true)

	recorder := serveGenAI(router, model, genaiUserAgent)

	body := recorder.Body.String()
	rest, ok := strings.CutPrefix(body, sse(first))
	if !ok || !strings.HasPrefix(rest, `{"error":{"code":503,`) || !strings.Contains(rest, `"status":"UNAVAILABLE"`) {
		t.Fatalf("body = %q, want the partial answer and one retryable error line", body)
	}
}
