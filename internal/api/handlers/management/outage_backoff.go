package management

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// GetOutageBackoffs lists the credentials and models whose held requests are
// waiting out an upstream outage, for a live view in the panel.
func (h *Handler) GetOutageBackoffs(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	now := time.Now()
	c.JSON(http.StatusOK, gin.H{
		"observed_at": now.UTC().Format(time.RFC3339Nano),
		"backoffs":    h.authManager.OutageBackoffs(now),
	})
}
