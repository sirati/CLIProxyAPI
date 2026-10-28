package management

import (
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// StartUseOnce restarts routing for one window without changing the saved minimum.
func (h *Handler) StartUseOnce(c *gin.Context) {
	var req struct {
		Name      string   `json:"name"`
		AuthIndex string   `json:"auth_index"`
		Target    *float64 `json:"target"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" || (req.Target != nil && (math.IsNaN(*req.Target) || math.IsInf(*req.Target, 0) || *req.Target < 0 || *req.Target > 100)) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name and a target from 0 to 100 are required"})
		return
	}
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	auth, _ := h.lookupAuthFile(req.Name, req.AuthIndex)
	if auth == nil || auth.AuthSourceKind() == coreauth.AuthSourceConfig {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}
	if coreauth.IsPluginVirtualAuth(auth) || (auth.Provider != "claude" && auth.Provider != "codex") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "use once supports Claude and Codex auth files"})
		return
	}
	updated, committed, errModify := h.authManager.Modify(c.Request.Context(), auth.ID, auth.RegistrationEpoch, func(current *coreauth.Auth) error {
		if current.Metadata == nil {
			current.Metadata = make(map[string]any)
		}
		current.Metadata[coreauth.MetadataUseOnce] = true
		delete(current.Metadata, "use_once_retired")
		delete(current.Metadata, "use_once_last_remaining")
		delete(current.Metadata, "use_once_baseline")
		delete(current.Metadata, coreauth.MetadataUseOnceTarget)
		if req.Target != nil {
			current.Metadata[coreauth.MetadataUseOnceTarget] = *req.Target
		}
		current.Disabled = false
		current.Metadata["disabled"] = false
		if current.Status == coreauth.StatusDisabled {
			current.Status = coreauth.StatusActive
		}
		if current.Quota.Reason == "credential_quota" && strings.HasPrefix(current.StatusMessage, "usage guard: ") {
			current.Quota = coreauth.QuotaState{}
			current.Unavailable = false
			current.NextRetryAfter = time.Time{}
		}
		if strings.HasPrefix(current.StatusMessage, "use once: ") || strings.HasPrefix(current.StatusMessage, "usage guard: ") {
			current.StatusMessage = ""
		}
		return nil
	})
	if errModify != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to persist use once"})
		return
	}
	if !committed {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}
	if h.postAuthPersistHook != nil {
		if errHook := h.postAuthPersistHook(c.Request.Context(), updated); errHook != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish use once"})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "disabled": false})
}
