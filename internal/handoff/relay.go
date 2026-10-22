package handoff

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

// Transfer turns a detached Conn into something another process can own:
// the socket itself when it is a plain socket, or, for a socket this process
// cannot give away (TLS), one end of a socketpair while a pump in this
// process copies plaintext between the TLS session and the other end. The
// pump keeps running after the handoff until either side closes; it makes no
// decisions and touches no shared state.
func Transfer(c *Conn) (file *os.File, carry []byte, err error) {
	if _, ok := c.raw.(interface{ File() (*os.File, error) }); ok {
		return c.Extract()
	}
	return startRelay(c)
}

var relays sync.WaitGroup

// WaitRelays blocks until every relay pump has ended.
func WaitRelays() { relays.Wait() }

func startRelay(c *Conn) (*os.File, []byte, error) {
	c.mu.Lock()
	if !c.detached {
		c.mu.Unlock()
		return nil, nil, errors.New("handoff: relay before detach")
	}
	for c.reading {
		c.cond.Wait()
	}
	carry := append([]byte(nil), c.pending...)
	c.pending = nil
	c.mu.Unlock()

	fds, errPair := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if errPair != nil {
		return nil, nil, errPair
	}
	peer := os.NewFile(uintptr(fds[0]), "handoff-relay-peer")
	localFile := os.NewFile(uintptr(fds[1]), "handoff-relay-local")
	local, errConn := net.FileConn(localFile)
	_ = localFile.Close()
	if errConn != nil {
		_ = peer.Close()
		return nil, nil, errConn
	}

	raw := c.raw
	_ = raw.SetReadDeadline(time.Time{})
	relays.Add(1)
	go func() {
		defer relays.Done()
		// A websocket ends with a close handshake, so the first side to stop
		// ends the session: close both instead of waiting for the other.
		ended := make(chan struct{}, 2)
		go func() {
			_, _ = io.Copy(local, raw)
			ended <- struct{}{}
		}()
		go func() {
			_, _ = io.Copy(raw, local)
			ended <- struct{}{}
		}()
		<-ended
		_ = raw.Close()
		_ = local.Close()
		<-ended
		log.Debugf("handoff: relay to %s ended", raw.RemoteAddr())
	}()
	// The bytes already read from TLS were never delivered here; the new
	// owner gets them as carry, ahead of what the pump forwards.
	return peer, carry, nil
}
