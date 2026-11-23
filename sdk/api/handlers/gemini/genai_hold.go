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
// holds such a stream open until the upstream answers, however long that
// takes, and the client never sees the failures:
//
//   - An overloaded or unreachable upstream is waited out at the gate of the
//     request's credential and model (coreauth.WithOutageHold), which sends
//     one request at a time while the outage lasts.
//   - Other transient failures before the answer starts, such as every
//     credential cooling down, are retried here.
//   - When the upstream breaks off mid-answer, the client keeps what it has
//     received and the proxy asks the upstream to continue from there: the
//     request goes out again with that text as the start of the model's turn,
//     and the continuation joins the same stream. An answer that already
//     holds a tool call ends there instead, since the client must run the
//     tool before anything can continue.
//
// Two client limits shape this. The client gives up when response headers
// take longer than 60 seconds, so a held or slow stream sends its headers
// first; once they are out it waits as long as the stream stays open. And the
// SDK's stream parser accepts only "data:" lines and bare JSON error lines:
// an SSE comment or "event: error" line ends the turn with "invalid stream
// chunk", while a bare error line becomes an ordinary API error that the
// client retries.
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
// Such a retry is answered locally while every credential still cools, so it
// costs the upstream nothing; the cap only bounds how late a cooldown lifted
// early, for example by a usage refresh, is noticed.
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

// genaiStopChunk ends an answer that holds a tool call where it broke off.
var genaiStopChunk = []byte(`data: {"candidates":[{"content":{"role":"model","parts":[{"text":""}]},"finishReason":"STOP","index":0}]}` + "\n\n")

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

// genaiAnswer is what the client has received of the model's answer.
type genaiAnswer struct {
	text     strings.Builder // visible text, without thoughts
	toolCall bool
}

func (a *genaiAnswer) add(chunk []byte) {
	for _, part := range ojson.GetBytes(chunk, "candidates.0.content.parts").Array() {
		switch {
		case part.Get("functionCall").Exists():
			a.toolCall = true
		case part.Get("thought").Bool():
		case part.Get("text").Exists():
			a.text.WriteString(part.Get("text").String())
		}
	}
}

// genaiContinueInstruction follows the partial answer in a continuation:
// Antigravity refuses a request that ends with a model turn ("Requests ending
// with a model turn are not supported"), so the answer cannot simply be
// prefilled.
const genaiContinueInstruction = "Your previous reply was cut off by a network error. Continue it exactly where it stopped, character for character: do not repeat anything already written and add nothing before the continuation."

// genaiContinuation is the request that continues an answer from its text:
// the original request, the text as the model's turn, and an instruction to
// go on from there.
func genaiContinuation(rawJSON []byte, text string) []byte {
	if text == "" {
		return rawJSON
	}
	doc := ojson.NewDoc(rawJSON)
	if errSet := doc.SetRaw("contents.-1", `{"role":"model","parts":[{"text":""}]}`); errSet != nil {
		return rawJSON
	}
	last := len(ojson.GetBytes(doc.Bytes(), "contents").Array()) - 1
	if errSet := doc.Set("contents."+strconv.Itoa(last)+".parts.0.text", text); errSet != nil {
		return rawJSON
	}
	if errSet := doc.SetRaw("contents.-1", `{"role":"user","parts":[{"text":""}]}`); errSet != nil {
		return rawJSON
	}
	if errSet := doc.Set("contents."+strconv.Itoa(last+1)+".parts.0.text", genaiContinueInstruction); errSet != nil {
		return rawJSON
	}
	return doc.Bytes()
}

// handleGenAIStream serves streamGenerateContent with alt=sse to a genai SDK
// client, holding the stream through transient failures (see above).
func (h *GeminiAPIHandler) handleGenAIStream(c *gin.Context, flusher http.Flusher, modelName string, rawJSON []byte) {
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	execCtx := coreauth.WithOutageHold(cliCtx)
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
	logger := log.WithField("request_id", logging.GetGinRequestID(c))

	type attempt struct {
		data    <-chan []byte
		headers http.Header
		errs    <-chan *interfaces.ErrorMessage
	}
	var answer genaiAnswer
	payload := rawJSON
	continuing := false // payload continues a partial answer
	heldSince := time.Time{}
	for failures := 0; ; {
		started := make(chan attempt, 1)
		go func(payload []byte) {
			data, headers, errs := h.ExecuteStreamWithAuthManager(execCtx, h.HandlerType(), modelName, payload, "")
			started <- attempt{data: data, headers: headers, errs: errs}
		}(payload)

		var run attempt
		for waiting := true; waiting; {
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

		// Relay this attempt until it ends or fails.
		var errMsg *interfaces.ErrorMessage
		delivered := false
		for errMsg == nil && (run.data != nil || run.errs != nil) {
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
					if pending, has := handlers.PendingStreamError(run.errs); has && pending != nil {
						errMsg = pending
						continue
					}
					commit(run.headers)
					cliCancel(nil)
					return
				}
				commit(run.headers)
				_, _ = c.Writer.Write([]byte("data: "))
				_, _ = c.Writer.Write(chunk)
				_, _ = c.Writer.Write([]byte("\n\n"))
				flusher.Flush()
				answer.add(chunk)
				if !delivered && !heldSince.IsZero() {
					logger.Infof("gemini: %s answers a genai client again after %d retries (held %s)", modelName, failures, time.Since(heldSince).Round(time.Second))
					heldSince = time.Time{}
				}
				delivered = true
				failures = 0
			}
		}
		if errMsg == nil {
			errMsg = &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: errors.New("upstream stream ended without a response")}
		}

		switch {
		// After a binary handoff this process decides nothing, so it cannot
		// retry. An unanswered request is re-run by the new process; a held
		// stream ends with a retryable error, and the client's retry reaches
		// the new process over its idle, moved connection.
		case !genaiTransient(errMsg) || handoff.Frozen():
			if !committed {
				h.WriteErrorResponse(c, errMsg)
				cliCancel(errMsg.Error)
				return
			}
			h.LoggingAPIResponseError(c, errMsg)
			if continuing && !delivered && !handoff.Frozen() {
				// A continuation the upstream refuses must not end the
				// turn: the client retries the whole call instead.
				errMsg = &interfaces.ErrorMessage{StatusCode: http.StatusServiceUnavailable, Error: errMsg.Error}
			}
			_, _ = c.Writer.Write(genaiErrorLine(errMsg))
			flusher.Flush()
			cliCancel(errMsg.Error)
			return
		case answer.toolCall:
			logger.Infof("gemini: %s broke off for a genai client after a tool call (%s); ending the answer there", modelName, genaiLogText(errMsg))
			h.LoggingAPIResponseError(c, errMsg)
			_, _ = c.Writer.Write(genaiStopChunk)
			flusher.Flush()
			cliCancel(errMsg.Error)
			return
		}

		failures++
		wait := genaiRetryWait(errMsg, failures)
		if delivered {
			logger.Infof("gemini: %s broke off mid-answer for a genai client (%s); continuing from the %d characters sent, in %s", modelName, genaiLogText(errMsg), answer.text.Len(), wait)
		} else if heldSince.IsZero() {
			heldSince = time.Now()
			logger.Infof("gemini: holding %s for a genai client after a transient failure (status %d: %s); retrying in %s", modelName, genaiErrorStatus(errMsg), genaiLogText(errMsg), wait)
		} else {
			logger.Debugf("gemini: %s still failing for a genai client (retry %d, status %d: %s); retrying in %s", modelName, failures, genaiErrorStatus(errMsg), genaiLogText(errMsg), wait)
		}
		payload = genaiContinuation(rawJSON, answer.text.String())
		continuing = answer.text.Len() > 0
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
