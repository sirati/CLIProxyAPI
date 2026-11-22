package handlers

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type levelVariantExecutor struct {
	mu     sync.Mutex
	models []string
}

func (e *levelVariantExecutor) Identifier() string { return "antigravity" }

func (e *levelVariantExecutor) Execute(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (coreexecutor.Response, error) {
	e.mu.Lock()
	e.models = append(e.models, req.Model)
	e.mu.Unlock()
	return coreexecutor.Response{Payload: []byte(`{"candidates":[]}`)}, nil
}

func (e *levelVariantExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (e *levelVariantExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *levelVariantExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *levelVariantExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

// registerLevelVariants registers an Antigravity-style credential serving the
// given model IDs and returns a handler routing to it.
func registerLevelVariants(t *testing.T, models ...string) (*BaseAPIHandler, *levelVariantExecutor) {
	t.Helper()
	manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
	exec := &levelVariantExecutor{}
	manager.RegisterExecutor(exec)
	id := "level-variant-" + uuid.NewString()
	if _, err := manager.Register(context.Background(), &coreauth.Auth{ID: id, Provider: "antigravity", Status: coreauth.StatusActive}); err != nil {
		t.Fatal(err)
	}
	infos := make([]*registry.ModelInfo, 0, len(models))
	for _, model := range models {
		infos = append(infos, &registry.ModelInfo{ID: model})
	}
	registry.GetGlobalRegistry().RegisterClient(id, "antigravity", infos)
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	return NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager), exec
}

func geminiThinking(level string) []byte {
	return []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"` + level + `"}}}`)
}

func TestLevelVariantModelResolvesGeminiAPIBaseNames(t *testing.T) {
	base := "lv-" + uuid.NewString()
	single := "lv-single-" + uuid.NewString()
	h, _ := registerLevelVariants(t, base+"-low", base+"-high", single+"-xhigh", "lv-exact-"+base)

	for _, tc := range []struct {
		name, model string
		body        []byte
		want        string
	}{
		{"request level picks its variant", base, geminiThinking("LOW"), base + "-low"},
		{"thinking budget maps to a level", base, []byte(`{"generationConfig":{"thinkingConfig":{"thinkingBudget":1024}}}`), base + "-low"},
		{"no level picks the highest", base, []byte(`{}`), base + "-high"},
		{"missing level variant picks the highest", base, geminiThinking("XHIGH"), base + "-high"},
		{"single variant serves every level", single, geminiThinking("LOW"), single + "-xhigh"},
		{"suffix is kept", base + "(low)", []byte(`{}`), base + "-low(low)"},
		{"registered name is unchanged", "lv-exact-" + base, geminiThinking("LOW"), "lv-exact-" + base},
		{"unknown name is unchanged", "lv-unknown-" + base, geminiThinking("LOW"), "lv-unknown-" + base},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := h.levelVariantModel(tc.model, "gemini", tc.body); got != tc.want {
				t.Fatalf("levelVariantModel(%q) = %q, want %q", tc.model, got, tc.want)
			}
		})
	}
}

// agy in Gemini-API-key mode sends "gemini-4-neon" plus thinkingLevel XHIGH
// for what Antigravity's catalog names "gemini-4-neon-xhigh".
func TestGeminiAPIBaseNameRoutesToLevelVariant(t *testing.T) {
	base := "lv-neon-" + uuid.NewString()
	h, exec := registerLevelVariants(t, base+"-xhigh")
	if _, _, errMsg := h.ExecuteWithAuthManager(context.Background(), "gemini", base, geminiThinking("XHIGH"), ""); errMsg != nil {
		t.Fatalf("ExecuteWithAuthManager() error = %v", errMsg.Error)
	}
	if len(exec.models) != 1 || exec.models[0] != base+"-xhigh" {
		t.Fatalf("executor models = %v, want [%s-xhigh]", exec.models, base)
	}
	// An excluded variant is not registered, so its base name stays unknown.
	if _, _, errMsg := h.ExecuteWithAuthManager(context.Background(), "gemini", "lv-excluded-"+base, geminiThinking("HIGH"), ""); errMsg == nil || errMsg.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown base name: %+v, want 400 model_not_found", errMsg)
	}
}
