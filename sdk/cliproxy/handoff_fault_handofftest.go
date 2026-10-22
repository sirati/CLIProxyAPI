//go:build handofftest

package cliproxy

import (
	"fmt"
	"os"
)

// handoffFault fails the named takeover step when CLIPROXY_HANDOFF_FAULT
// names it. This file is only compiled with -tags handofftest.
func handoffFault(point string) error {
	if os.Getenv("CLIPROXY_HANDOFF_FAULT") == point {
		return fmt.Errorf("injected fault at %s", point)
	}
	return nil
}
