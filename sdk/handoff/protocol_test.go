package handoff

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func pair(t *testing.T) (*Conn, *Conn) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ctl.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan *net.UnixConn, 1)
	go func() {
		conn, _ := listener.AcceptUnix()
		accepted <- conn
	}()
	client, err := Dial(path)
	if err != nil {
		t.Fatal(err)
	}
	server := NewConn(<-accepted)
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return client, server
}

func TestMessagesCarryFilesAndLargePayloads(t *testing.T) {
	client, server := pair(t)
	file, err := os.CreateTemp(t.TempDir(), "fd")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString("through the socket")
	big := strings.Repeat("x", 3<<20) // larger than one socket buffer
	go func() {
		_ = client.SendJSON(TypeItem, KindConnHTTP1, "c1", map[string]string{"big": big}, file)
		_ = client.SendJSON(TypeHello, "", "", Hello{Protocols: []int{1}, Capabilities: []string{KindListenerTCP}})
	}()

	msg, files, err := server.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Type != TypeItem || msg.Kind != KindConnHTTP1 || msg.ID != "c1" || len(files) != 1 {
		t.Fatalf("got %+v with %d files", msg, len(files))
	}
	var payload map[string]string
	if err := msg.Decode(&payload); err != nil || payload["big"] != big {
		t.Fatalf("payload lost: %v", err)
	}
	if _, err := files[0].Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(files[0])
	if !bytes.Equal(data, []byte("through the socket")) {
		t.Fatalf("received fd reads %q", data)
	}

	msg, files, err = server.Recv()
	if err != nil || len(files) != 0 {
		t.Fatalf("hello: %v, %d files", err, len(files))
	}
	var hello Hello
	if err := msg.Decode(&hello); err != nil || !hello.Supports(KindListenerTCP) || hello.Supports(KindRelayPlain) {
		t.Fatalf("hello = %+v, %v", hello, err)
	}
}

func TestPeerErrorIsReturned(t *testing.T) {
	client, server := pair(t)
	go func() { _ = client.Send(Message{Type: TypeError, Error: "boom"}) }()
	_, _, err := server.Recv()
	var peerErr *PeerError
	if !errors.As(err, &peerErr) || peerErr.Message != "boom" {
		t.Fatalf("err = %v", err)
	}
}
