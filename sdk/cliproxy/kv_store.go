package cliproxy

import (
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/kvstore"
	log "github.com/sirupsen/logrus"
)

func (s *Service) applyKVStoreConfig(cfg *config.Config) {
	if s == nil {
		if errShutdown := kvstore.ShutdownLocal(); errShutdown != nil {
			log.Errorf("failed to shutdown local kv store: %v", errShutdown)
		}
		return
	}
	s.kvStoreManaged.Store(true)
	if cfg == nil || cfg.Home.Enabled || home.Current() != nil || cfg.KVStore.Disabled {
		if errShutdown := kvstore.ShutdownLocal(); errShutdown != nil {
			log.Errorf("failed to shutdown local kv store: %v", errShutdown)
		}
		return
	}
	_ = kvstore.ConfigureLocal(true, cfg.EffectiveKVStorePath())
}

// releaseKVStoreForHandoff closes the local store so the process taking over
// can open it; this process finishes its requests with in-memory storage.
// It reports whether the store must be reopened if the takeover fails.
func (s *Service) releaseKVStoreForHandoff() bool {
	// A config reload in this process must not take the store back.
	if s == nil || !s.kvStoreManaged.Swap(false) {
		return false
	}
	if errShutdown := kvstore.ShutdownLocal(); errShutdown != nil {
		log.Errorf("handoff: releasing local kv store: %v", errShutdown)
	}
	return true
}

func (s *Service) shutdownKVStore() error {
	if s == nil {
		return nil
	}
	s.kvStoreManaged.Store(false)
	return kvstore.ShutdownLocal()
}
