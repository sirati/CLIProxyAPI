package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestStartUseOnceRestartsWithoutChangingMinimumOrUpstreamCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := coreauth.NewManager(nil, nil, nil)
	retry := time.Now().Add(time.Hour)
	auth := &coreauth.Auth{ID: "once.json", FileName: "once.json", Provider: "claude", Status: coreauth.StatusActive,
		Unavailable: true, NextRetryAfter: retry, Quota: coreauth.QuotaState{Exceeded: true, Reason: "credential_quota", NextRecoverAt: retry},
		Metadata: map[string]any{"use_once": true, "use_once_retired": "stopped", "use_once_last_remaining": 60.0, "min_remaining": 25.0}}
	if _, err := m.Register(context.Background(), auth); err != nil {
		t.Fatal(err)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, m)
	request := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v8/management/credentials/use-once", strings.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		h.StartUseOnce(c)
		return rec
	}
	for _, target := range []string{"-1", "101", "\"bad\""} {
		if rec := request(`{"name":"once.json","target":` + target + `}`); rec.Code != 400 {
			t.Fatalf("invalid target accepted: %s", rec.Body.String())
		}
	}
	if rec := request(`{"name":"once.json","target":0}`); rec.Code != 200 {
		t.Fatalf("restart: %d %s", rec.Code, rec.Body.String())
	}
	updated, _ := m.GetByID(auth.ID)
	if coreauth.UseOnceStopped(updated) || updated.Disabled || updated.Metadata["min_remaining"] != 25.0 || updated.Metadata["use_once_target"] != 0.0 {
		t.Fatalf("bad restart: %+v", updated)
	}
	if !updated.Unavailable || !updated.NextRetryAfter.Equal(retry) || updated.Quota.Reason != "credential_quota" {
		t.Fatal("upstream cooldown was erased")
	}
	if _, ok := updated.Metadata["use_once_last_remaining"]; ok {
		t.Fatal("old baseline retained")
	}
	if rec := request(`{"name":"once.json"}`); rec.Code != 200 {
		t.Fatalf("saved target: %s", rec.Body.String())
	}
	updated, _ = m.GetByID(auth.ID)
	if _, ok := updated.Metadata["use_once_target"]; ok {
		t.Fatal("override retained instead of saved target")
	}
}

func TestStoppedUseOnceCredentialRemainsVisibleInV8(t *testing.T) {
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	auth := &coreauth.Auth{ID: "stopped.json", FileName: "stopped.json", Provider: "claude", Status: coreauth.StatusActive,
		Attributes: map[string]string{"path": "/auths/stopped.json"},
		Metadata:   map[string]any{"use_once": true, "use_once_retired": "stopped", "min_remaining": 25.0}}
	entry := h.buildAuthFileEntry(auth, authFileQuotaContext{v8: true})
	if entry == nil || entry["disabled"] != false || entry["routing_stopped"] != true || entry["use_once"] != true || entry["min_remaining"] != 25.0 {
		t.Fatalf("stopped credential not visible/enabled: %+v", entry)
	}
}
