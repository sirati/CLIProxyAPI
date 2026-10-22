package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestManagementHTMLChangesReachABrowserDespiteEqualMtime(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "management.html")
	storeTime := time.Unix(1, 0) // every Nix store file
	write := func(body string) {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, storeTime, storeTime); err != nil {
			t.Fatal(err)
		}
	}
	get := func(header, value string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodGet, "/management.html", nil)
		if header != "" {
			c.Request.Header.Set(header, value)
		}
		serveManagementHTML(c, path)
		return recorder
	}

	write("<html>old panel</html>")
	first := get("", "")
	etag := first.Header().Get("ETag")
	if first.Code != http.StatusOK || etag == "" || first.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("first load: %d etag=%q headers=%v", first.Code, etag, first.Header())
	}
	if again := get("If-None-Match", etag); again.Code != http.StatusNotModified {
		t.Fatalf("unchanged panel: %d, want 304", again.Code)
	}

	write("<html>new panel!</html>") // a new build, same size class, same mtime
	for _, revalidate := range [][2]string{
		{"If-None-Match", etag},
		{"If-Modified-Since", storeTime.UTC().Format(http.TimeFormat)},
	} {
		resp := get(revalidate[0], revalidate[1])
		if resp.Code != http.StatusOK || resp.Body.String() != "<html>new panel!</html>" {
			t.Fatalf("%s after an upgrade: %d %q, want the new panel", revalidate[0], resp.Code, resp.Body.String())
		}
	}
}
