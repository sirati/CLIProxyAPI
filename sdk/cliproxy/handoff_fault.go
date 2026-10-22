//go:build !handofftest

package cliproxy

// handoffFault injects failures into a takeover; only test builds have any.
func handoffFault(string) error { return nil }
