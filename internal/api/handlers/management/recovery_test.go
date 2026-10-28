package management

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestRecoveryPollingAndSSE(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := coreauth.NewManager(nil, nil, nil)
	const model = "management-recovery-model"
	a := &coreauth.Auth{ID: "recovery-management.json", Provider: "claude", Status: coreauth.StatusActive, Metadata: map[string]any{"use_once": true, "use_once_retired": "stopped"}}
	if _, err := m.Register(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
	err := m.TrackRecovery(&coreauth.Error{Code: "auth_unavailable", HTTPStatus: 503, Message: "unavailable"}, []string{"claude"}, model)
	id := strings.TrimSuffix(strings.TrimPrefix(coreauth.RecoveryMarker(err), " [cliproxy-recovery:"), "]")
	cfg := &config.Config{Routing: config.RoutingConfig{RecoveryNotifications: true}}
	h := NewHandlerWithoutConfigFilePath(cfg, m)
	router := gin.New()
	router.GET("/recovery/:id", h.GetRecovery)
	poll := httptest.NewRecorder()
	router.ServeHTTP(poll, httptest.NewRequest("GET", "/recovery/"+id, nil))
	if poll.Code != 200 || !strings.Contains(poll.Body.String(), `"state":"waiting"`) {
		t.Fatalf("poll: %d %s", poll.Code, poll.Body.String())
	}
	server := httptest.NewServer(router)
	defer server.Close()
	resp, errGet := http.Get(server.URL + "/recovery/" + id + "?watch=true")
	if errGet != nil {
		t.Fatal(errGet)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("headers: %v", resp.Header)
	}
	scanner := bufio.NewScanner(resp.Body)
	readEvent := func() string {
		t.Helper()
		event := ""
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "event: ") {
				event = strings.TrimPrefix(line, "event: ")
			}
			if line == "" {
				return event
			}
		}
		t.Fatalf("stream closed before event: %v", scanner.Err())
		return ""
	}
	if event := readEvent(); event != "waiting" {
		t.Fatalf("initial event=%s", event)
	}
	// Release after subscription. No clock sleeps or model retries are needed.
	if _, _, errModify := m.Modify(context.Background(), a.ID, 0, func(current *coreauth.Auth) error { delete(current.Metadata, "use_once_retired"); return nil }); errModify != nil {
		t.Fatal(errModify)
	}
	if event := readEvent(); event != "available" {
		t.Fatalf("recovery event=%s", event)
	}
	// A reconnect after recovery immediately returns the current state.
	poll = httptest.NewRecorder()
	router.ServeHTTP(poll, httptest.NewRequest("GET", "/recovery/"+id+"?watch=true", nil))
	if !strings.Contains(poll.Body.String(), "event: available") {
		t.Fatalf("reconnect: %s", poll.Body.String())
	}
	for _, tc := range []struct {
		enabled bool
		id      string
	}{{true, "unknown"}, {false, id}} {
		cfg.Routing.RecoveryNotifications = tc.enabled
		h.SetConfig(cfg)
		poll = httptest.NewRecorder()
		router.ServeHTTP(poll, httptest.NewRequest("GET", "/recovery/"+tc.id, nil))
		if poll.Code != 404 {
			t.Fatalf("disabled/unknown: %d", poll.Code)
		}
	}
	// Ensure JSON status never discloses credentials or provider account identity.
	status, _ := m.Recovery(id, time.Now())
	encoded, _ := json.Marshal(status)
	if strings.Contains(string(encoded), a.ID) {
		t.Fatal("credential identity leaked")
	}
}
