package handoff

import (
	"net"
	"os"
)

// Notify sends a state string to systemd's notification socket, if the
// process runs under a unit with NOTIFY_SOCKET set. Without one it does
// nothing.
func Notify(state string) error {
	path := os.Getenv("NOTIFY_SOCKET")
	if path == "" {
		return nil
	}
	if path[0] == '@' {
		path = "\x00" + path[1:]
	}
	conn, errDial := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if errDial != nil {
		return errDial
	}
	defer func() { _ = conn.Close() }()
	_, errWrite := conn.Write([]byte(state))
	return errWrite
}
