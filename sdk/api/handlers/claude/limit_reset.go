package claude

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
)

// claudeLimitError is a Claude error body. Its field order is part of the
// format: clients match the text `{"type":"error","error":{"type":...`.
type claudeLimitError struct {
	Type  string                 `json:"type"`
	Error claudeLimitErrorDetail `json:"error"`
}

type claudeLimitErrorDetail struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// writeLimitResponse answers exhausted Claude credentials so that clients
// learn when to resume. Claude Code reads Anthropic's rate-limit headers only
// from a subscription or gateway login, so through a base URL the reset has to
// travel in the message: a 503 the client does not retry, carrying a wrapped
// 429 and "(reset after 5h 59m 30s)", which T3 Code turns into a timed resume.
func (h *ClaudeCodeAPIHandler) writeLimitResponse(c *gin.Context, msg *interfaces.ErrorMessage) bool {
	reset, answer := h.LimitReset(msg, "claude")
	if answer != handlers.LimitReached {
		return false
	}
	reset = reset.Round(time.Second)
	upstream, _ := json.Marshal(claudeLimitError{Type: "error", Error: claudeLimitErrorDetail{
		Type:    "rate_limit_error",
		Message: "Every Claude credential is rate limited or out of quota.",
	}})
	hours, minutes, seconds := int(reset.Hours()), int(reset.Minutes())%60, int(reset.Seconds())%60
	body, _ := json.Marshal(claudeLimitError{Type: "error", Error: claudeLimitErrorDetail{
		Type:    "rate_limit_error",
		Message: fmt.Sprintf("[cliproxy] [429]: %s (reset after %dh %dm %ds)", upstream, hours, minutes, seconds),
	}})

	appendClaudeAPIResponse(c, body)
	c.Header("Content-Type", "application/json")
	c.Header("Retry-After", strconv.Itoa(int(reset.Seconds())))
	c.Header("x-should-retry", "false")
	c.Status(http.StatusServiceUnavailable)
	_, _ = c.Writer.Write(body)
	return true
}
