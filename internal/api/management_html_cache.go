package api

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// serveManagementHTML serves the panel with an ETag of its content instead of
// a Last-Modified date. A panel from the Nix store always has the same mtime
// (the epoch), so a browser asking "modified since?" would keep the old panel
// after every upgrade.
func serveManagementHTML(c *gin.Context, filePath string) {
	body, etag, errRead := managementHTMLContent(filePath)
	if errRead != nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	c.Header("Cache-Control", "no-cache")
	c.Header("ETag", etag)
	for _, candidate := range strings.Split(c.GetHeader("If-None-Match"), ",") {
		if strings.TrimSpace(candidate) == etag {
			c.Status(http.StatusNotModified)
			c.Writer.WriteHeaderNow()
			return
		}
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", body)
}

var managementHTMLCache struct {
	sync.Mutex
	path    string
	size    int64
	modTime time.Time
	body    []byte
	etag    string
}

// managementHTMLContent reads the panel once per change of path, size or
// mtime; the auto-updater rewrites the file in place.
func managementHTMLContent(filePath string) ([]byte, string, error) {
	info, errStat := os.Stat(filePath)
	if errStat != nil {
		return nil, "", errStat
	}
	cache := &managementHTMLCache
	cache.Lock()
	defer cache.Unlock()
	if cache.path == filePath && cache.size == info.Size() && cache.modTime.Equal(info.ModTime()) && cache.body != nil {
		return cache.body, cache.etag, nil
	}
	body, errRead := os.ReadFile(filePath)
	if errRead != nil {
		return nil, "", errRead
	}
	sum := sha256.Sum256(body)
	cache.path, cache.size, cache.modTime = filePath, info.Size(), info.ModTime()
	cache.body, cache.etag = body, `"`+hex.EncodeToString(sum[:16])+`"`
	return cache.body, cache.etag, nil
}
