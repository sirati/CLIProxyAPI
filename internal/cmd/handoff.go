package cmd

import "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy"

// handoffConfig is set from the command line before the service starts.
var handoffConfig cliproxy.HandoffConfig

// SetHandoffConfig configures seamless upgrades for StartService.
func SetHandoffConfig(config cliproxy.HandoffConfig) { handoffConfig = config }
