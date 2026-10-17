package auth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type exhaustedClaudeUsageExecutor struct{}

func (exhaustedClaudeUsageExecutor) Identifier() string { return "claude" }

func (exhaustedClaudeUsageExecutor) Execute(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (exhaustedClaudeUsageExecutor) ExecuteStream(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}

func (exhaustedClaudeUsageExecutor) Refresh(_ context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	return auth, nil
}

func (exhaustedClaudeUsageExecutor) CountTokens(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (exhaustedClaudeUsageExecutor) HttpRequest(context.Context, *cliproxyauth.Auth, *http.Request) (*http.Response, error) {
	body := `{"seven_day":{"utilization":100,"resets_at":"2026-10-20T00:00:00Z"}}`
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
}

// A retired use_once credential is written to its auth file as disabled, keeps
// its tokens, and loads disabled after a restart.
func TestUseOnceRetirementPersistsToAuthFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "claude-once.json")
	raw := `{"type":"claude","email":"once@example.com","access_token":"at","refresh_token":"rt","use_once":true}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(dir)
	loaded, err := store.List(context.Background())
	if err != nil || len(loaded) != 1 {
		t.Fatalf("List() = %d auths, %v", len(loaded), err)
	}

	manager := cliproxyauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(exhaustedClaudeUsageExecutor{})
	if _, err := manager.Register(context.Background(), loaded[0]); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.StartUsageGuard(ctx)
	defer manager.StopUsageGuard()

	var saved map[string]any
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, errRead := os.ReadFile(path)
		if errRead == nil && json.Unmarshal(data, &saved) == nil && saved["disabled"] == true {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("auth file never marked disabled: %s", data)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if saved["refresh_token"] != "rt" || saved["use_once_retired"] == nil {
		t.Fatalf("auth file lost its login or the retire reason: %v", saved)
	}

	reloaded, err := store.List(context.Background())
	if err != nil || len(reloaded) != 1 || !reloaded[0].Disabled {
		t.Fatalf("after restart: %d auths, disabled=%v, err=%v", len(reloaded), len(reloaded) == 1 && reloaded[0].Disabled, err)
	}
}
