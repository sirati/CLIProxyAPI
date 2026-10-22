package handoff

import (
	"errors"
	"net"
	"os"
	"syscall"
	"time"
)

func notifySocket() (*net.UnixConn, error) {
	path := os.Getenv("NOTIFY_SOCKET")
	if path == "" {
		return nil, nil
	}
	if path[0] == '@' {
		path = "\x00" + path[1:]
	}
	return net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
}

// Notify sends a state string to systemd's notification socket, if the
// process runs under a unit with NOTIFY_SOCKET set. Without one it does
// nothing.
func Notify(state string) error {
	conn, errDial := notifySocket()
	if conn == nil || errDial != nil {
		return errDial
	}
	defer func() { _ = conn.Close() }()
	_, errWrite := conn.Write([]byte(state))
	return errWrite
}

// NotifyBarrier waits until systemd has processed every notification sent
// before it (sd_notify_barrier). A takeover must not let the old main
// process exit before systemd follows MAINPID= to the new one.
func NotifyBarrier(timeout time.Duration) error {
	conn, errDial := notifySocket()
	if conn == nil || errDial != nil {
		return errDial
	}
	defer func() { _ = conn.Close() }()
	pipe := make([]int, 2)
	if errPipe := syscall.Pipe2(pipe, syscall.O_CLOEXEC|syscall.O_NONBLOCK); errPipe != nil {
		return errPipe
	}
	readEnd := os.NewFile(uintptr(pipe[0]), "notify-barrier")
	defer func() { _ = readEnd.Close() }()
	_, _, errWrite := conn.WriteMsgUnix([]byte("BARRIER=1"), syscall.UnixRights(pipe[1]), nil)
	_ = syscall.Close(pipe[1])
	if errWrite != nil {
		return errWrite
	}
	// systemd closes its copy of the write end once it got this far. The
	// read end is non-blocking, so os.File polls it and honours the deadline.
	if errDeadline := readEnd.SetReadDeadline(time.Now().Add(timeout)); errDeadline != nil {
		return errDeadline
	}
	buf := make([]byte, 1)
	if _, errRead := readEnd.Read(buf); errRead != nil && !errors.Is(errRead, os.ErrClosed) {
		if errors.Is(errRead, os.ErrDeadlineExceeded) {
			return errors.New("handoff: systemd did not confirm the notification")
		}
	}
	return nil
}
