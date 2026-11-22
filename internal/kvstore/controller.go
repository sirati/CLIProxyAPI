package kvstore

import (
	"errors"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

var (
	controllerMu sync.RWMutex
	homeProvider func() (KVClient, bool, error)
	localEnabled bool
	localStore   *BoltStore
	localPath    string
	localErr     error
	// localWaiting is set while another process holds the database file,
	// usually the one this process took over from. Callers get in-memory
	// storage until a background retry opens the file.
	localWaiting bool
	// localGen ends a background retry once the store is reconfigured.
	localGen uint64
)

// Opening waits briefly for the file lock. A lock held longer, for example by
// a predecessor that is still draining, is retried in the background.
var (
	localOpenTimeout  = 200 * time.Millisecond
	localRetryTimeout = time.Second
)

// RegisterHomeProvider registers the callback used by CurrentKVClient to inspect
// whether Home mode is active and obtain the active Home KV client.
func RegisterHomeProvider(fn func() (KVClient, bool, error)) {
	controllerMu.Lock()
	homeProvider = fn
	controllerMu.Unlock()
}

// ConfigureLocal enables, reconfigures, or disables the local bbolt-backed KV store.
func ConfigureLocal(enabled bool, rawPath string) error {
	controllerMu.Lock()
	defer controllerMu.Unlock()

	if !enabled {
		return shutdownLocalLocked()
	}

	resolvedPath, errResolve := ResolvePath(rawPath)
	if errResolve != nil {
		if localStore != nil {
			if errClose := localStore.Close(); errClose != nil {
				log.Errorf("local kv store close failed: %v", errClose)
			}
			localStore = nil
			localPath = ""
		}
		localGen++
		localWaiting = false
		localEnabled = true
		localErr = errResolve
		log.Errorf("local kv store resolve path failed: %v", errResolve)
		return errResolve
	}

	if (localStore != nil || localWaiting) && localPath == resolvedPath && localErr == nil {
		localEnabled = true
		return nil
	}

	if localStore != nil {
		if errClose := localStore.Close(); errClose != nil {
			log.Errorf("local kv store close failed: %v", errClose)
		}
		localStore = nil
		localPath = ""
	}
	localGen++
	localWaiting = false

	store, errOpen := openBoltStore(resolvedPath, localOpenTimeout)
	if errors.Is(errOpen, bbolt.ErrTimeout) {
		localEnabled = true
		localErr = nil
		localPath = resolvedPath
		localWaiting = true
		log.Warnf("local kv store %s is locked by another process; using in-memory storage until it is released", resolvedPath)
		go retryLocal(localGen, resolvedPath, localRetryTimeout)
		return nil
	}
	if errOpen != nil {
		localEnabled = true
		localErr = errOpen
		log.Errorf("local kv store open failed (path=%s): %v", resolvedPath, errOpen)
		return errOpen
	}

	localEnabled = true
	localStore = store
	localPath = resolvedPath
	localErr = nil
	return nil
}

// ShutdownLocal closes any open local BoltStore and resets local KV state to disabled.
func ShutdownLocal() error {
	controllerMu.Lock()
	defer controllerMu.Unlock()
	return shutdownLocalLocked()
}

func shutdownLocalLocked() error {
	var errClose error
	if localStore != nil {
		errClose = localStore.Close()
	}
	localEnabled = false
	localStore = nil
	localPath = ""
	localErr = nil
	localWaiting = false
	localGen++
	return errClose
}

// retryLocal opens a database file held by another process as soon as that
// process releases it, unless the store was reconfigured in the meantime.
func retryLocal(gen uint64, path string, lockTimeout time.Duration) {
	for {
		store, errOpen := openBoltStore(path, lockTimeout)
		controllerMu.Lock()
		if gen != localGen || !localWaiting {
			controllerMu.Unlock()
			if store != nil {
				if errClose := store.Close(); errClose != nil {
					log.Errorf("local kv store close failed: %v", errClose)
				}
			}
			return
		}
		switch {
		case errOpen == nil:
			localStore, localWaiting = store, false
			controllerMu.Unlock()
			log.Infof("local kv store %s opened after the other process released it", path)
			return
		case !errors.Is(errOpen, bbolt.ErrTimeout):
			localErr, localWaiting = errOpen, false
			controllerMu.Unlock()
			log.Errorf("local kv store open failed (path=%s): %v", path, errOpen)
			return
		}
		controllerMu.Unlock()
	}
}

// CurrentKVClient returns the active KVClient using the three-tier routing order:
//  1. Home KV when Home mode is active (never falling back to local bbolt).
//  2. Local BoltStore when server.kv-store.disabled is false.
//  3. (nil, false, nil) for default in-memory storage.
func CurrentKVClient() (KVClient, bool, error) {
	controllerMu.RLock()
	provider := homeProvider
	controllerMu.RUnlock()

	if provider != nil {
		if client, homeMode, errHome := provider(); homeMode {
			return client, true, errHome
		}
	}

	controllerMu.RLock()
	defer controllerMu.RUnlock()

	if !localEnabled || localWaiting {
		return nil, false, nil
	}
	if localErr != nil {
		return nil, true, fmt.Errorf("local kv store unavailable: %w", localErr)
	}
	if localStore == nil {
		return nil, true, fmt.Errorf("local kv store unavailable: %w", errStoreClosed)
	}
	return localStore, true, nil
}
