package claude

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	"github.com/tidwall/gjson"
)

func TestClaudeLimitPreservesRecoveryMarker(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager := coreauth.NewManager(nil, nil, nil)
	err := manager.TrackRecovery(coreauth.NewModelCooldownError("claude-test", "claude", time.Hour), []string{"claude"}, "claude-test")
	marker := coreauth.RecoveryMarker(err)
	if marker == "" {
		t.Fatal("missing ticket")
	}
	h := NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: 429, Error: err})
	message := gjson.GetBytes(recorder.Body.Bytes(), "error.message").String()
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(message, marker) || !strings.Contains(message, "reset after") {
		t.Fatalf("lost marker or reset: %d %s", recorder.Code, message)
	}
}
