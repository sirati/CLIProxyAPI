package management

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// GetRecovery supports polling and a single long-lived SSE subscription. All
// availability checks are local; waiting never sends a request to a provider.
func (h *Handler) GetRecovery(c *gin.Context) {
	h.mu.Lock()
	cfg := h.cfg.CloneForRuntime()
	h.mu.Unlock()
	if cfg == nil || !cfg.Routing.RecoveryNotifications || cfg.Home.Enabled {
		c.JSON(http.StatusNotFound, gin.H{"error": "recovery notifications disabled"})
		return
	}
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	status, ok := h.authManager.Recovery(c.Param("id"), time.Now())
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown or expired recovery ID"})
		return
	}
	c.Header("Cache-Control", "no-store")
	if c.Query("watch") != "true" {
		c.JSON(http.StatusOK, status)
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("X-Accel-Buffering", "no")
	send := func(event string, value any) bool {
		body, err := json.Marshal(value)
		if err != nil {
			return false
		}
		if _, errWrite := fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", event, body); errWrite != nil {
			return false
		}
		c.Writer.Flush()
		return true
	}
	if !send(status.State, status) || status.State == "available" {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	heartbeat := 0
	for {
		select {
		case <-c.Request.Context().Done():
			return
		case now := <-ticker.C:
			status, ok = h.authManager.Recovery(c.Param("id"), now)
			if !ok {
				send("expired", gin.H{"id": c.Param("id")})
				return
			}
			if status.State == "available" {
				send("available", status)
				return
			}
			heartbeat++
			if heartbeat%15 == 0 && !send("waiting", status) {
				return
			}
		}
	}
}
