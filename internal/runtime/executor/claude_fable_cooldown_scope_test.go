package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestFableRateLimitDoesNotCoolOpusWithoutSharedRejection(t *testing.T) {
	for _, path := range []string{"execute", "stream", "count_tokens", "fast_execute", "fast_stream"} {
		for _, sharedWindow := range []string{"", "5h", "7d"} {
			t.Run(path+"/shared_"+sharedWindow, func(t *testing.T) {
				ctx := t.Context()
				var fableAttempts, opusAttempts int
				transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
					body, errRead := io.ReadAll(req.Body)
					if errRead != nil {
						return nil, errRead
					}
					headers := http.Header{"Content-Type": {"application/json"}}
					status := http.StatusOK
					response := `{"id":"opus-ok","type":"message","role":"assistant","model":"claude-opus-5-5","content":[{"type":"text","text":"ok"}]}`
					if strings.Contains(string(body), `"model":"claude-fable-5-1"`) {
						fableAttempts++
						headers.Set("Anthropic-Ratelimit-Unified-Status", "rejected")
						headers.Set("Anthropic-Ratelimit-Unified-7d_oi-Status", "rejected")
						headers.Set("Retry-After", "120")
						if sharedWindow != "" {
							headers.Set("Anthropic-Ratelimit-Unified-"+sharedWindow+"-Status", "rejected")
						}
						status = http.StatusTooManyRequests
						response = `{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."}}`
					} else {
						opusAttempts++
					}
					return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(response)), Request: req}, nil
				})
				ctx = context.WithValue(ctx, "cliproxy.roundtripper", http.RoundTripper(transport))
				manager := cliproxyauth.NewManager(nil, nil, nil)
				manager.SetRetryConfig(0, 0, 0)
				manager.RegisterExecutor(NewClaudeExecutor(&config.Config{}))
				auth := &cliproxyauth.Auth{ID: t.Name(), Provider: "claude", Attributes: map[string]string{"api_key": "sk-ant-api03-fable-scope-test"}}
				if _, err := manager.Register(ctx, auth); err != nil {
					t.Fatal(err)
				}
				reg := registry.GetGlobalRegistry()
				reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "claude-fable-5-1"}, {ID: "claude-opus-5-5"}})
				t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
				payload := `{"model":"claude-fable-5-1","max_tokens":16,"messages":[{"role":"user","content":"test"}]}`
				if strings.HasPrefix(path, "fast_") {
					payload = `{"model":"claude-fable-5-1","max_tokens":16,"speed":"fast","messages":[{"role":"user","content":"test"}]}`
				}
				req := cliproxyexecutor.Request{Model: "claude-fable-5-1", Payload: []byte(payload)}
				opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, ResponseFormat: sdktranslator.FormatClaude}
				var err error
				switch path {
				case "stream", "fast_stream":
					_, err = manager.ExecuteStream(ctx, []string{"claude"}, req, opts)
				case "count_tokens":
					_, err = manager.ExecuteCount(ctx, []string{"claude"}, req, opts)
				default:
					_, err = manager.Execute(ctx, []string{"claude"}, req, opts)
				}
				if err == nil || fableAttempts != 1 {
					t.Fatalf("Fable failure = %v, attempts = %d", err, fableAttempts)
				}
				current, _ := manager.GetByID(auth.ID)
				wantCredentialCooldown := sharedWindow != ""
				if got := current.Quota.Reason == "credential_quota"; got != wantCredentialCooldown {
					t.Fatalf("credential cooldown = %v, want %v", got, wantCredentialCooldown)
				}
				_, err = manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{
					Model: "claude-opus-5-5", Payload: []byte(`{"model":"claude-opus-5-5","max_tokens":16,"messages":[{"role":"user","content":"test"}]}`),
				}, opts)
				if !wantCredentialCooldown && (err != nil || opusAttempts != 1) {
					t.Fatalf("Fable rejection prevented Opus: error=%v attempts=%d", err, opusAttempts)
				}
				if wantCredentialCooldown && (err == nil || opusAttempts != 0) {
					t.Fatal("explicit shared allowance exhaustion did not block Opus")
				}
			})
		}
	}
}

func TestFableRateLimitScopeUsesFinalPayloadModel(t *testing.T) {
	for _, path := range []string{"execute", "stream", "count_tokens", "fast_execute", "fast_stream"} {
		for _, target := range []string{"claude-fable-5-1", "claude-opus-5-5"} {
			t.Run(path+"/"+target, func(t *testing.T) {
				requested := "claude-fable-5-1"
				if target == requested {
					requested = "claude-opus-5-5"
				}
				transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
					body, err := io.ReadAll(req.Body)
					if err != nil {
						return nil, err
					}
					if got := gjson.GetBytes(body, "model").String(); got != target {
						t.Fatalf("wire model = %s, want %s", got, target)
					}
					return &http.Response{StatusCode: http.StatusTooManyRequests, Request: req,
						Header: http.Header{"Anthropic-Ratelimit-Unified-Status": {"rejected"}, "Retry-After": {"120"}},
						Body:   io.NopCloser(strings.NewReader(`{"type":"error","error":{"type":"rate_limit_error","message":"rate limit exceeded"}}`)),
					}, nil
				})
				ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(transport))
				cfg := &config.Config{Payload: config.PayloadConfig{Override: []config.PayloadRule{{
					Models: []config.PayloadModelRule{{Name: requested}}, Params: map[string]any{"model": target},
				}}}}
				executor := NewClaudeExecutor(cfg)
				auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-ant-api03-model-scope-test"}}
				payload := `{"max_tokens":16,"messages":[{"role":"user","content":"test"}]}`
				if strings.HasPrefix(path, "fast_") {
					payload = `{"max_tokens":16,"speed":"fast","messages":[{"role":"user","content":"test"}]}`
				}
				req := cliproxyexecutor.Request{Model: requested, Payload: []byte(payload)}
				opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, ResponseFormat: sdktranslator.FormatClaude}
				var err error
				switch path {
				case "stream", "fast_stream":
					_, err = executor.ExecuteStream(ctx, auth, req, opts)
				case "count_tokens":
					_, err = executor.CountTokens(ctx, auth, req, opts)
				default:
					_, err = executor.Execute(ctx, auth, req, opts)
				}
				var scoped interface{ IsCredentialScoped() bool }
				if !errors.As(err, &scoped) || scoped.IsCredentialScoped() != (target == "claude-opus-5-5") {
					t.Fatalf("error scope did not follow final payload model %s: %v", target, err)
				}
			})
		}
	}
}
