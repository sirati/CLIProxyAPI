package config

import (
	"slices"
	"testing"
)

func TestParseConfigBytesKnownMissingModels(t *testing.T) {
	for name, yaml := range map[string]string{
		"v8 routing section": "config-version: 8\nrouting:\n  strategy: round-robin\n  known-missing-models:\n    - gemini-3.8-flash\n    - gemini-3.1-flash-lite-preview\n",
		"legacy top level":   "known-missing-models:\n  - gemini-3.8-flash\n  - gemini-3.1-flash-lite-preview\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, errParse := ParseConfigBytes([]byte(yaml))
			if errParse != nil {
				t.Fatalf("ParseConfigBytes() error = %v", errParse)
			}
			want := []string{"gemini-3.8-flash", "gemini-3.1-flash-lite-preview"}
			if !slices.Equal(cfg.KnownMissingModels, want) {
				t.Fatalf("KnownMissingModels = %v, want %v", cfg.KnownMissingModels, want)
			}
		})
	}
}
