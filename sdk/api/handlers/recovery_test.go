package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestRecoveryErrorOptInAndHTTPSerialization(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := coreauth.NewManager(nil, nil, nil)
	h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, m)
	original := &coreauth.Error{Code: "auth_unavailable", HTTPStatus: 503, Message: "no auth available"}
	if got := h.enrichExecutionError(original, []string{"claude"}, "test"); coreauth.RecoveryMarker(got) != "" {
		t.Fatal("marker enabled by default")
	}
	m.SetConfig(&config.Config{Routing: config.RoutingConfig{RecoveryNotifications: true}})
	err := h.enrichExecutionError(original, []string{"claude"}, "test")
	marker := coreauth.RecoveryMarker(err)
	if marker == "" {
		t.Fatal("missing opt-in marker")
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusServiceUnavailable, Error: err})
	if !strings.Contains(recorder.Body.String(), marker) {
		t.Fatalf("marker lost: %s", recorder.Body.String())
	}
}
