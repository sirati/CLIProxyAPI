package executor

import (
	"context"
	"net/http"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// A request held through outages buffers its start even with bootstrap
// buffering off, so an overload before any output fails the attempt (and
// the auth manager can wait it out) instead of reaching the client as a
// stream that ends without response.completed.
func TestCodexExecutor_HeldRequestBuffersItsStartWithoutConfig(t *testing.T) {
	server := codexSSEServer(codexCreatedEvent, codexInProgressEvent, codexOverloadEvent)
	defer server.Close()

	req, opts := codexTestRequest()
	held := cliproxyauth.WithOutageHold(context.Background())
	result, err := NewCodexExecutor(codexBufferingConfig(false)).ExecuteStream(held, codexTestAuth(server.URL), req, opts)
	if err == nil || result != nil {
		t.Fatalf("result = %v err = %v, want the overload to fail the attempt", result, err)
	}
	if got := statusCodeFromTestError(t, err); got != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", got)
	}
}

func TestCodexWebsocketsExecutor_HeldRequestBuffersItsStartWithoutConfig(t *testing.T) {
	server := codexWebsocketServer(t, codexCreatedEvent, codexInProgressEvent, codexOverloadEvent)
	defer server.Close()

	req, opts := codexWebsocketRequest()
	held := cliproxyauth.WithOutageHold(context.Background())
	result, err := NewCodexWebsocketsExecutor(codexBufferingConfig(false)).ExecuteStream(held, codexTestAuth(server.URL), req, opts)
	if err == nil || result != nil {
		t.Fatalf("result = %v err = %v, want the overload to fail the attempt", result, err)
	}
}

// Without the hold, an unbuffered stream keeps delivering the overload in-stream.
func TestCodexExecutor_UnheldRequestKeepsTheConfiguredBehaviour(t *testing.T) {
	server := codexSSEServer(codexCreatedEvent, codexInProgressEvent, codexOverloadEvent)
	defer server.Close()

	req, opts := codexTestRequest()
	result, err := NewCodexExecutor(codexBufferingConfig(false)).ExecuteStream(context.Background(), codexTestAuth(server.URL), req, opts)
	if err != nil || result == nil {
		t.Fatalf("result = %v err = %v, want a stream", result, err)
	}
	if _, streamErr := drainChunks(result); streamErr == nil {
		t.Fatal("want the overload delivered inside the stream")
	}
}
