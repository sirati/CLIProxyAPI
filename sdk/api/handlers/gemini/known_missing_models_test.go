package gemini

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api/middleware"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// The Antigravity CLI asks for flash models no provider serves before every
// tool call, each time with the whole trajectory in the body. A model listed in
// known-missing-models is refused as before, but its refusal writes no error
// log; any other unknown model still does.
func TestKnownMissingModelIsRefusedWithoutAnErrorLog(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logsDir := t.TempDir()
	router := gin.New()
	router.Use(middleware.RequestLoggingMiddleware(logging.NewFileRequestLogger(false, logsDir, "", 5)))
	cfg := &sdkconfig.SDKConfig{KnownMissingModels: []string{"gemini-3.8-flash", "Gemini-3.1-Flash-Lite-Preview"}}
	h := NewGeminiAPIHandler(handlers.NewBaseAPIHandlers(cfg, coreauth.NewManager(nil, nil, nil)))
	router.POST("/v1beta/models/*action", h.GeminiHandler)

	post := func(model string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/v1beta/models/"+model+":streamGenerateContent?alt=sse",
			strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"rate this trajectory"}]}]}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("User-Agent", "google-genai-sdk/1.79.1 gl-go/go1.28")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		return recorder
	}
	errorLogs := func() []string {
		entries, errRead := os.ReadDir(logsDir)
		if errRead != nil {
			t.Fatalf("read logs dir: %v", errRead)
		}
		var names []string
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "error-") {
				names = append(names, entry.Name())
			}
		}
		return names
	}

	for _, model := range []string{"gemini-3.8-flash", "gemini-3.1-flash-lite-preview"} {
		recorder := post(model)
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "model_not_found") {
			t.Fatalf("%s: status = %d body = %q, want the usual 400 model_not_found", model, recorder.Code, recorder.Body.String())
		}
	}
	if names := errorLogs(); len(names) != 0 {
		t.Fatalf("known missing models wrote error logs: %v", names)
	}

	if recorder := post("gemini-0-unlisted"); recorder.Code != http.StatusBadRequest {
		t.Fatalf("unlisted model: status = %d, want 400", recorder.Code)
	}
	if names := errorLogs(); len(names) != 1 || !strings.Contains(names[0], "gemini-0-unlisted") {
		t.Fatalf("error logs = %v, want one for the unlisted model", names)
	}
}
