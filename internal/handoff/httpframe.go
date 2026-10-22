package handoff

import (
	"bytes"
	"strconv"
	"strings"
)

// httpFramer follows HTTP/1.1 request boundaries in the byte stream a server
// reads, so the connection wrapper knows when no part of a request is held by
// net/http. It understands Content-Length and chunked bodies; anything it
// cannot frame marks the stream opaque and the connection is never moved.
type httpFramer struct {
	state     httpFrameState
	header    []byte // header bytes of the current request
	remaining int64  // body or chunk-data bytes still to come
	chunkLine []byte // current chunk-size or trailer line
	opaque    bool
	requests  int
}

type httpFrameState int

const (
	httpIdle httpFrameState = iota // between requests
	httpHeader
	httpBody
	httpChunkSize
	httpChunkData
	httpChunkDataEnd // the CRLF after chunk data
	httpTrailer
)

const maxHeaderBytes = 1 << 20

func (f *httpFramer) atBoundary() bool { return f.state == httpIdle && !f.opaque }

// feed advances over delivered bytes.
func (f *httpFramer) feed(b []byte) {
	for len(b) > 0 && !f.opaque {
		switch f.state {
		case httpIdle:
			f.state = httpHeader
			f.header = f.header[:0]
		case httpHeader:
			idx := bytes.IndexByte(b, '\n')
			if idx < 0 {
				f.header = append(f.header, b...)
				b = nil
			} else {
				f.header = append(f.header, b[:idx+1]...)
				b = b[idx+1:]
				if bytes.HasSuffix(f.header, []byte("\r\n\r\n")) || bytes.HasSuffix(f.header, []byte("\n\n")) {
					f.headerDone()
				}
			}
			if len(f.header) > maxHeaderBytes {
				f.opaque = true
			}
		case httpBody, httpChunkData:
			n := int(min(int64(len(b)), f.remaining))
			f.remaining -= int64(n)
			b = b[n:]
			if f.remaining == 0 {
				if f.state == httpBody {
					f.endRequest()
				} else {
					f.state = httpChunkDataEnd
					f.chunkLine = f.chunkLine[:0]
				}
			}
		case httpChunkSize, httpChunkDataEnd, httpTrailer:
			idx := bytes.IndexByte(b, '\n')
			if idx < 0 {
				f.chunkLine = append(f.chunkLine, b...)
				b = nil
				if len(f.chunkLine) > maxHeaderBytes {
					f.opaque = true
				}
				continue
			}
			f.chunkLine = append(f.chunkLine, b[:idx+1]...)
			b = b[idx+1:]
			line := strings.TrimRight(string(f.chunkLine), "\r\n")
			f.chunkLine = f.chunkLine[:0]
			f.lineDone(line)
		}
	}
}

func (f *httpFramer) headerDone() {
	contentLength := int64(0)
	chunked := false
	lines := strings.Split(string(f.header), "\n")
	for _, line := range lines[1:] {
		name, value, ok := strings.Cut(strings.TrimRight(line, "\r"), ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "content-length":
			parsed, errParse := strconv.ParseInt(value, 10, 64)
			if errParse != nil || parsed < 0 {
				f.opaque = true
				return
			}
			contentLength = parsed
		case "transfer-encoding":
			if strings.Contains(strings.ToLower(value), "chunked") {
				chunked = true
			} else {
				f.opaque = true
				return
			}
		}
	}
	switch {
	case chunked:
		f.state = httpChunkSize
		f.chunkLine = f.chunkLine[:0]
	case contentLength > 0:
		f.state = httpBody
		f.remaining = contentLength
	default:
		f.endRequest()
	}
}

func (f *httpFramer) lineDone(line string) {
	switch f.state {
	case httpChunkSize:
		sizeText, _, _ := strings.Cut(line, ";")
		size, errParse := strconv.ParseInt(strings.TrimSpace(sizeText), 16, 64)
		if errParse != nil || size < 0 {
			f.opaque = true
			return
		}
		if size == 0 {
			f.state = httpTrailer
			return
		}
		f.state = httpChunkData
		f.remaining = size
	case httpChunkDataEnd:
		if line != "" {
			f.opaque = true
			return
		}
		f.state = httpChunkSize
	case httpTrailer:
		if line == "" {
			f.endRequest()
		}
	}
}

// endRequest returns to idle. After an upgrade request the caller switches
// the Conn to websocket framing once the server answers 101; a refused
// upgrade is just a request.
func (f *httpFramer) endRequest() {
	f.requests++
	f.state = httpIdle
}
