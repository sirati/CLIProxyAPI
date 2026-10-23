package openai

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
)

// Exhausted Codex credentials are answered the way the ChatGPT backend
// answers an exhausted account: a 429 usage_limit_reached with the reset, and
// x-codex-primary-* headers marking a full window. Codex then stops without
// retrying and reports the reset to its client, where T3 Code schedules the
// resume.

type codexLimitBody struct {
	Error codexLimitDetail `json:"error"`
}

type codexLimitDetail struct {
	Type            string `json:"type"`
	Message         string `json:"message"`
	ResetsAt        int64  `json:"resets_at"`
	ResetsInSeconds int64  `json:"resets_in_seconds"`
}

func (h *OpenAIResponsesAPIHandler) codexLimit(msg *interfaces.ErrorMessage) (codexLimitDetail, http.Header, bool) {
	if h == nil {
		return codexLimitDetail{}, nil, false
	}
	reset, ok := h.LimitReset(msg, "codex")
	if !ok {
		return codexLimitDetail{}, nil, false
	}
	seconds := int64(reset.Round(time.Second).Seconds())
	resetsAt := time.Now().Unix() + seconds
	detail := codexLimitDetail{
		Type:            "usage_limit_reached",
		Message:         "Every Codex credential has reached its usage limit.",
		ResetsAt:        resetsAt,
		ResetsInSeconds: seconds,
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	headers.Set("Retry-After", strconv.FormatInt(seconds, 10))
	headers.Set("X-Codex-Primary-Used-Percent", "100")
	headers.Set("X-Codex-Primary-Window-Minutes", "300")
	headers.Set("X-Codex-Primary-Reset-At", strconv.FormatInt(resetsAt, 10))
	return detail, headers, true
}

// WriteErrorResponse answers exhausted credentials with a Codex usage limit
// and every other error as the base handler does.
func (h *OpenAIResponsesAPIHandler) WriteErrorResponse(c *gin.Context, msg *interfaces.ErrorMessage) {
	if detail, headers, ok := h.codexLimit(msg); ok {
		body, _ := json.Marshal(codexLimitBody{Error: detail})
		msg = &interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, DirectResponse: true, Body: body, Headers: headers}
	}
	h.BaseAPIHandler.WriteErrorResponse(c, msg)
}

// writeResponsesWebsocketTerminalError sends exhausted credentials to the
// client as the websocket form of a usage limit, which a quota failure
// otherwise never shows: the client would reconnect and fail again.
func (h *OpenAIResponsesAPIHandler) writeResponsesWebsocketTerminalError(
	writer *responsesWebsocketWriter,
	wsTimelineLog websocketTimelineAppender,
	errMsg *interfaces.ErrorMessage,
	payload []byte,
) ([]byte, bool, error) {
	detail, headers, ok := h.codexLimit(errMsg)
	if !ok {
		return writeResponsesWebsocketTerminalError(writer, wsTimelineLog, errMsg, payload)
	}
	eventHeaders := make(map[string]string, len(headers))
	for key := range headers {
		eventHeaders[key] = headers.Get(key)
	}
	payload, _ = json.Marshal(map[string]any{
		"type":    wsEventTypeError,
		"status":  http.StatusTooManyRequests,
		"error":   detail,
		"headers": eventHeaders,
	})
	wrote, errClose := writer.closeWithPayload(payload)
	if wrote && wsTimelineLog != nil {
		wsTimelineLog.Append("response", payload, time.Now())
	}
	if errClose != nil {
		return payload, wrote, errClose
	}
	return payload, wrote, websocket.ErrCloseSent
}
