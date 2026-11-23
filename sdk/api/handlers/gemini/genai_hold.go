package gemini

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/clienterror"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/handoff"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// Clients built on Google's genai SDK, such as the Antigravity CLI (agy),
// fail a whole agent turn when a model call fails: agy retries an error only
// eight times over about two minutes, and nothing resumes the turn after
// that. A capacity outage or a cooling credential lasts longer. So the proxy
// holds such a stream open and retries transient failures itself until the
// upstream answers, and the client never sees them.
//
// Two client limits shape this. The client gives up when response headers
// take longer than 60 seconds, so a held or slow stream sends its headers
// first. And the SDK's stream parser accepts only "data:" lines and bare JSON
// error lines: an SSE comment or "event: error" line ends the turn with
// "invalid stream chunk", while a bare error line becomes an ordinary API
// error that the client retries.
var (
	genaiHeaderDeadline = 30 * time.Second
	genaiMaxRetryWait   = 30 * time.Second
	genaiFirstRetryWait = time.Second
)

// isGenAIClient reports whether the request comes from Google's genai SDK.
func isGenAIClient(c *gin.Context) bool {
	if c == nil || c.Request == nil {
		return false
	}
	for _, value := range []string{c.Request.UserAgent(), c.GetHeader("X-Goog-Api-Client")} {
		if strings.Contains(value, "google-genai-sdk/") {
			return true
		}
	}
	return false
}

// genaiStatusName is the google.rpc status name the genai SDK expects with code.
func genaiStatusName(code int) string {
	switch code {
	case http.StatusBadRequest:
		return "INVALID_ARGUMENT"
	case http.StatusUnauthorized:
		return "UNAUTHENTICATED"
	case http.StatusForbidden:
		return "PERMISSION_DENIED"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "ABORTED"
	case http.StatusTooManyRequests:
		return "RESOURCE_EXHAUSTED"
	case 499:
		return "CANCELLED"
	case http.StatusNotImplemented:
		return "NOT_IMPLEMENTED"
	case http.StatusServiceUnavailable:
		return "UNAVAILABLE"
	case http.StatusGatewayTimeout:
		return "DEADLINE_EXCEEDED"
	default:
		return "INTERNAL"
	}
}

// genaiErrorStatus is the HTTP status of a failed call.
func genaiErrorStatus(msg *interfaces.ErrorMessage) int {
	if msg == nil {
		return http.StatusInternalServerError
	}
	if msg.StatusCode > 0 {
		return msg.StatusCode
	}
	return clienterror.HTTPStatusFromError(msg.Error)
}

// genaiTransient reports whether a failed call is worth retrying unchanged:
// rate limits, capacity and server errors, and network failures. Errors that
// need the request or the account changed are not.
func genaiTransient(msg *interfaces.ErrorMessage) bool {
	if msg == nil || msg.DirectResponse {
		return false
	}
	err := msg.Error
	if errors.Is(err, context.Canceled) ||
		coreauth.IsContinuityTerminatedError(err) ||
		coreauth.IsRequestStopError(err) ||
		coreauth.IsTOSViolationError(err) ||
		coreauth.IsAntigravityValidationRequiredError(err) {
		return false
	}
	status := genaiErrorStatus(msg)
	return status == 0 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

// genaiRetryWait is how long to wait before retrying a transient failure: the
// wait the error names, such as a credential's cooldown, otherwise a backoff.
// It never exceeds genaiMaxRetryWait, so a cooldown lifted early, for example
// by a usage refresh, is noticed; a retry while every credential still cools
// is answered locally without an upstream call.
func genaiRetryWait(msg *interfaces.ErrorMessage, failures int) time.Duration {
	wait := genaiFirstRetryWait << min(max(failures-1, 0), 5)
	if hint, ok := genaiRetryHint(msg); ok {
		wait = hint
	}
	return min(max(wait, genaiFirstRetryWait), genaiMaxRetryWait)
}

func genaiRetryHint(msg *interfaces.ErrorMessage) (time.Duration, bool) {
	if msg == nil {
		return 0, false
	}
	if reset, ok := coreauth.ModelCooldownResetIn(msg.Error); ok && reset > 0 {
		return reset, true
	}
	var retryAfter interface{ RetryAfter() *time.Duration }
	if errors.As(msg.Error, &retryAfter) && retryAfter != nil {
		if d := retryAfter.RetryAfter(); d != nil && *d > 0 {
			return *d, true
		}
	}
	for _, headers := range []http.Header{msg.Headers, msg.Addon} {
		if seconds, err := strconv.Atoi(strings.TrimSpace(headers.Get("Retry-After"))); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second, true
		}
	}
	return 0, false
}

// genaiErrorLine is a failed call written into a stream that has started, in
// the form the genai SDK turns into an API error. A transient failure is
// reported as 503 UNAVAILABLE, which the client retries.
func genaiErrorLine(msg *interfaces.ErrorMessage) []byte {
	status := genaiErrorStatus(msg)
	if genaiTransient(msg) || status <= 0 {
		status = http.StatusServiceUnavailable
	}
	message := http.StatusText(status)
	if msg != nil && msg.Error != nil && msg.Error.Error() != "" {
		message = msg.Error.Error()
	}
	doc := ojson.NewDoc([]byte(`{"error":{"code":0,"message":"","status":""}}`))
	_ = doc.Set("error.code", status)
	_ = doc.Set("error.message", message)
	_ = doc.Set("error.status", genaiStatusName(status))
	return append(doc.Bytes(), '\n', '\n')
}

func genaiLogText(msg *interfaces.ErrorMessage) string {
	text := "unknown error"
	if msg != nil && msg.Error != nil {
		text = msg.Error.Error()
	}
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 240 {
		text = text[:240] + "…"
	}
	return text
}

// handleGenAIStream serves streamGenerateContent with alt=sse to a genai SDK
// client, holding the stream through transient failures (see above).
func (h *GeminiAPIHandler) handleGenAIStream(c *gin.Context, flusher http.Flusher, modelName string, rawJSON []byte) {
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	committed := false
	commit := func(upstreamHeaders http.Header) {
		if committed {
			return
		}
		committed = true
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("Access-Control-Allow-Origin", "*")
		handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
		c.Status(http.StatusOK)
		c.Writer.WriteHeaderNow()
		flusher.Flush()
	}
	headerDeadline := time.NewTimer(genaiHeaderDeadline)
	defer headerDeadline.Stop()
	clientGone := c.Request.Context().Done()

	type attempt struct {
		data    <-chan []byte
		headers http.Header
		errs    <-chan *interfaces.ErrorMessage
	}
	logger := log.WithField("request_id", logging.GetGinRequestID(c))
	heldSince := time.Time{}
	for failures := 0; ; {
		started := make(chan attempt, 1)
		go func() {
			data, headers, errs := h.ExecuteStreamWithAuthManager(cliCtx, h.HandlerType(), modelName, rawJSON, "")
			started <- attempt{data: data, headers: headers, errs: errs}
		}()

		var run attempt
		var errMsg *interfaces.ErrorMessage
		waiting := true
		for waiting {
			select {
			case <-clientGone:
				cliCancel(c.Request.Context().Err())
				return
			case <-headerDeadline.C:
				commit(nil)
			case run = <-started:
				waiting = false
			}
		}
		// The first item is ready once the bootstrap has returned.
		for run.data != nil || run.errs != nil {
			select {
			case <-clientGone:
				cliCancel(c.Request.Context().Err())
				return
			case msg, ok := <-run.errs:
				if !ok {
					run.errs = nil
					continue
				}
				errMsg = msg
			case chunk, ok := <-run.data:
				if !ok {
					run.data = nil
					if pending, has := handlers.PendingStreamError(run.errs); has {
						errMsg = pending
					}
					if errMsg == nil {
						// Closed without data or error.
						commit(run.headers)
						cliCancel(nil)
						return
					}
					break
				}
				commit(run.headers)
				_, _ = c.Writer.Write([]byte("data: "))
				_, _ = c.Writer.Write(chunk)
				_, _ = c.Writer.Write([]byte("\n\n"))
				flusher.Flush()
				if !heldSince.IsZero() {
					logger.Infof("gemini: %s recovered for a genai client after %d retries (held %s)", modelName, failures, time.Since(heldSince).Round(time.Second))
				}
				h.forwardGeminiStream(c, flusher, "", func(err error) { cliCancel(err) }, run.data, run.errs)
				return
			}
			if errMsg != nil {
				break
			}
		}
		if errMsg == nil {
			errMsg = &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New("upstream stream ended without a response")}
		}

		// After a binary handoff this process decides nothing, so it cannot
		// retry. An unanswered request is re-run by the new process; a held
		// stream ends with a retryable error, and the client's retry reaches
		// the new process over its idle, moved connection.
		if !genaiTransient(errMsg) || handoff.Frozen() {
			if committed {
				h.LoggingAPIResponseError(c, errMsg)
				_, _ = c.Writer.Write(genaiErrorLine(errMsg))
				flusher.Flush()
			} else {
				h.WriteErrorResponse(c, errMsg)
			}
			cliCancel(errMsg.Error)
			return
		}
		failures++
		wait := genaiRetryWait(errMsg, failures)
		if heldSince.IsZero() {
			heldSince = time.Now()
			logger.Infof("gemini: holding %s for a genai client after a transient failure (status %d: %s); retrying in %s", modelName, genaiErrorStatus(errMsg), genaiLogText(errMsg), wait)
		} else {
			logger.Debugf("gemini: %s still failing for a genai client (retry %d, status %d: %s); retrying in %s", modelName, failures, genaiErrorStatus(errMsg), genaiLogText(errMsg), wait)
		}
		// The client sees headers before its header timeout, and nothing else
		// until the upstream answers.
		commit(nil)
		timer := time.NewTimer(wait)
		select {
		case <-clientGone:
			timer.Stop()
			cliCancel(c.Request.Context().Err())
			return
		case <-timer.C:
		}
	}
}
