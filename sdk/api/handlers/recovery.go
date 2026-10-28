package handlers

// enrichExecutionError tracks recovery after the final route and failure are known.
func (h *BaseAPIHandler) enrichExecutionError(err error, providers []string, model string) error {
	err = enrichAuthSelectionError(err, providers, model)
	if h.AuthManager != nil && h.AuthManager.RecoveryNotificationsEnabled() {
		return h.AuthManager.TrackRecovery(err, providers, model)
	}
	return err
}
