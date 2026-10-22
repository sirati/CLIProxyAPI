// Package handoff is the wire protocol two CLIProxyAPI processes use to hand a
// running service from an old binary to a new one without dropping a socket.
//
// The protocol is deliberately small and version-stable; see PROTOCOL.md. Both
// sides exchange a Hello listing the item kinds they understand, and the old
// side only ever sends kinds the new side listed. Anything else stays with the
// old process until it closes, so any two versions can hand off safely.
package handoff

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
)

// ProtocolVersion is the only framing version so far. Item payloads are
// versioned by their kind name instead (".v1", ".v2", ...).
const ProtocolVersion = 1

// Message types.
const (
	TypeHello    = "hello"     // both directions, first message
	TypeTakeover = "takeover"  // new -> old: freeze and send everything
	TypeItem     = "item"      // old -> new: one transferable thing
	TypeStateEnd = "state-end" // old -> new: all state items were sent
	TypeReady    = "ready"     // new -> old: serving; old is now dead
	TypeEvent    = "event"     // old -> new: a side effect to apply
	TypeDone     = "done"      // old -> new: nothing transferable is left
	TypeError    = "error"     // either direction: fatal, abort the handoff
)

// Item and event kinds. A new behaviour always gets a new kind.
const (
	KindListenerTCP     = "listener.tcp.v1"
	KindControlListener = "control.listener.v1"
	KindConnHTTP1       = "conn.http1.v1"
	KindConnWSResponses = "conn.ws.responses.v1"
	KindRelayPlain      = "relay.plain.v1"
	KindStateAffinity   = "state.affinity.v1"
	KindStateCooldown   = "state.cooldown.v1"
	KindStateUsageQueue = "state.usagequeue.v1"
	KindEventResult     = "event.result.v1"
	KindEventUsage      = "event.usage.v1"
)

// maxFrame bounds one message; state snapshots are well below this.
const maxFrame = 256 << 20

// maxFDs is the per-message fd limit (the kernel's SCM_MAX_FD is 253).
const maxFDs = 16

// Message is one protocol frame. Data holds the kind-specific payload.
type Message struct {
	V     int             `json:"v"`
	Type  string          `json:"type"`
	Kind  string          `json:"kind,omitempty"`
	ID    string          `json:"id,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Hello is the payload of TypeHello.
type Hello struct {
	Protocols    []int    `json:"protocols"`
	Binary       string   `json:"binary"`
	PID          int      `json:"pid"`
	Capabilities []string `json:"capabilities"`
}

// Supports reports whether the peer listed kind.
func (h Hello) Supports(kind string) bool {
	for _, capability := range h.Capabilities {
		if capability == kind {
			return true
		}
	}
	return false
}

// Conn is a framed control connection over a unix stream socket. Each frame
// is a 4-byte big-endian length and a JSON Message, written with a single
// sendmsg so that fds travel attached to the frame's first byte.
type Conn struct {
	conn    *net.UnixConn
	writeMu sync.Mutex
	readMu  sync.Mutex
}

// NewConn wraps an established unix connection.
func NewConn(conn *net.UnixConn) *Conn { return &Conn{conn: conn} }

// Dial connects to a control socket.
func Dial(path string) (*Conn, error) {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	return NewConn(conn), nil
}

// Close closes the connection.
func (c *Conn) Close() error { return c.conn.Close() }

// Send writes msg with files attached. The files stay open in the sender;
// the receiver gets its own descriptors.
func (c *Conn) Send(msg Message, files ...*os.File) error {
	if len(files) > maxFDs {
		return fmt.Errorf("handoff: %d fds exceed the per-message limit %d", len(files), maxFDs)
	}
	msg.V = ProtocolVersion
	body, errMarshal := json.Marshal(msg)
	if errMarshal != nil {
		return errMarshal
	}
	if len(body) > maxFrame {
		return fmt.Errorf("handoff: message of %d bytes exceeds %d", len(body), maxFrame)
	}
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame, uint32(len(body)))
	copy(frame[4:], body)
	var oob []byte
	if len(files) > 0 {
		fds := make([]int, len(files))
		for i, file := range files {
			fds[i] = int(file.Fd())
		}
		oob = syscall.UnixRights(fds...)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	n, _, errWrite := c.conn.WriteMsgUnix(frame, oob, nil)
	if errWrite != nil {
		return errWrite
	}
	if n < len(frame) {
		// The fds went with the first chunk; the rest is plain bytes.
		_, errWrite = c.conn.Write(frame[n:])
	}
	return errWrite
}

// SendJSON sends a message whose Data is payload marshalled as JSON.
func (c *Conn) SendJSON(msgType, kind, id string, payload any, files ...*os.File) error {
	msg := Message{Type: msgType, Kind: kind, ID: id}
	if payload != nil {
		data, errMarshal := json.Marshal(payload)
		if errMarshal != nil {
			return errMarshal
		}
		msg.Data = data
	}
	return c.Send(msg, files...)
}

// Recv reads one message and the files that came with it.
func (c *Conn) Recv() (Message, []*os.File, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	var msg Message
	header := make([]byte, 4)
	oob := make([]byte, syscall.CmsgSpace(maxFDs*4))
	n, oobn, _, _, errRead := c.conn.ReadMsgUnix(header, oob)
	if errRead != nil {
		return msg, nil, errRead
	}
	files, errFiles := parseRights(oob[:oobn])
	if errFiles != nil {
		return msg, nil, errFiles
	}
	if n < 4 {
		if _, errRead = io.ReadFull(c.conn, header[n:]); errRead != nil {
			closeFiles(files)
			return msg, nil, errRead
		}
	}
	size := binary.BigEndian.Uint32(header)
	if size > maxFrame {
		closeFiles(files)
		return msg, nil, fmt.Errorf("handoff: frame of %d bytes exceeds %d", size, maxFrame)
	}
	body := make([]byte, size)
	if _, errRead = io.ReadFull(c.conn, body); errRead != nil {
		closeFiles(files)
		return msg, nil, errRead
	}
	if errUnmarshal := json.Unmarshal(body, &msg); errUnmarshal != nil {
		closeFiles(files)
		return msg, nil, errUnmarshal
	}
	if msg.Type == TypeError {
		closeFiles(files)
		return msg, nil, &PeerError{Message: msg.Error}
	}
	return msg, files, nil
}

// PeerError is an error the other side reported.
type PeerError struct{ Message string }

func (e *PeerError) Error() string { return "handoff: peer error: " + e.Message }

func parseRights(oob []byte) ([]*os.File, error) {
	if len(oob) == 0 {
		return nil, nil
	}
	messages, errParse := syscall.ParseSocketControlMessage(oob)
	if errParse != nil {
		return nil, errParse
	}
	var files []*os.File
	for i := range messages {
		fds, errRights := syscall.ParseUnixRights(&messages[i])
		if errRights != nil {
			closeFiles(files)
			return nil, errRights
		}
		for _, fd := range fds {
			syscall.CloseOnExec(fd)
			files = append(files, os.NewFile(uintptr(fd), "handoff-fd"))
		}
	}
	return files, nil
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		_ = file.Close()
	}
}

// Decode unmarshals msg.Data into out.
func (m Message) Decode(out any) error {
	if len(m.Data) == 0 {
		return errors.New("handoff: message has no data")
	}
	return json.Unmarshal(m.Data, out)
}
