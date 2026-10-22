package handoff

import "encoding/binary"

// frameTracker follows websocket frame boundaries in a byte stream so a
// reader can be stopped exactly between frames. It never buffers: callers ask
// how many of the next bytes belong to the current frame, then feed what they
// actually delivered.
type frameTracker struct {
	header     [14]byte
	headerLen  int   // header bytes seen for the current frame
	headerNeed int   // full header length once known, 0 while unknown
	remaining  int64 // payload bytes still to come once the header is complete
	// fragmented is set while a data message spans several frames.
	fragmented bool
	// frames counts complete frames, for tests and diagnostics.
	frames int
}

// atBoundary reports whether no part of a frame is outstanding and no data
// message is half delivered.
func (t *frameTracker) atBoundary() bool {
	return t.headerLen == 0 && !t.fragmented
}

// inFrame reports whether part of a frame has been delivered.
func (t *frameTracker) inFrame() bool { return t.headerLen > 0 }

// limit returns how many bytes of next may be delivered without crossing the
// end of the current frame.
func (t *frameTracker) limit(next []byte) int {
	shadow := *t
	n := 0
	for n < len(next) {
		step := shadow.stepLen(next[n:])
		shadow.feedStep(next[n : n+step])
		n += step
		if shadow.headerLen == 0 {
			// A frame just ended; stop there.
			return n
		}
	}
	return n
}

// feed advances the tracker over delivered bytes.
func (t *frameTracker) feed(delivered []byte) {
	for len(delivered) > 0 {
		step := t.stepLen(delivered)
		t.feedStep(delivered[:step])
		delivered = delivered[step:]
	}
}

// stepLen is how many bytes of b the current phase (header or payload) can
// consume in one step.
func (t *frameTracker) stepLen(b []byte) int {
	if t.headerNeed == 0 || t.headerLen < t.headerNeed {
		// Header phase: take one byte at a time until the length is known,
		// then the rest of the header.
		if t.headerNeed == 0 {
			return 1
		}
		return min(len(b), t.headerNeed-t.headerLen)
	}
	return int(min(int64(len(b)), t.remaining))
}

func (t *frameTracker) feedStep(b []byte) {
	if t.headerNeed == 0 || t.headerLen < t.headerNeed {
		copy(t.header[t.headerLen:], b)
		t.headerLen += len(b)
		if t.headerNeed == 0 && t.headerLen >= 2 {
			t.headerNeed = headerLength(t.header[0], t.header[1])
		}
		if t.headerNeed != 0 && t.headerLen == t.headerNeed {
			t.remaining = payloadLength(t.header[:t.headerLen])
			if t.remaining == 0 {
				t.endFrame()
			}
		}
		return
	}
	t.remaining -= int64(len(b))
	if t.remaining == 0 {
		t.endFrame()
	}
}

func (t *frameTracker) endFrame() {
	first := t.header[0]
	fin := first&0x80 != 0
	opcode := first & 0x0f
	if opcode < 0x8 {
		// Data frame (text, binary, continuation); control frames may sit
		// between fragments and change nothing.
		t.fragmented = !fin
	}
	t.headerLen, t.headerNeed, t.remaining = 0, 0, 0
	t.frames++
}

func headerLength(_ byte, second byte) int {
	n := 2
	switch second & 0x7f {
	case 126:
		n += 2
	case 127:
		n += 8
	}
	if second&0x80 != 0 {
		n += 4 // masking key
	}
	return n
}

func payloadLength(header []byte) int64 {
	switch header[1] & 0x7f {
	case 126:
		return int64(binary.BigEndian.Uint16(header[2:4]))
	case 127:
		return int64(binary.BigEndian.Uint64(header[2:10]) & (1<<63 - 1))
	default:
		return int64(header[1] & 0x7f)
	}
}
