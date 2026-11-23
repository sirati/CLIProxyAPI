package auth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

type outageTestError struct {
	status int
	msg    string
}

func (e outageTestError) Error() string   { return e.msg }
func (e outageTestError) StatusCode() int { return e.status }

func TestOutageGateBacksOffToFiveMinutesAndRampsBackUp(t *testing.T) {
	s := &outageGates{gates: map[string]*outageGate{}}
	const key = "auth\x00model"
	now := time.Unix(0, 0)
	a, b := s.newTicket(), s.newTicket()

	if next := s.admit(key, a, now); !next.dispatch || next.probe {
		t.Fatalf("open gate: %+v, want a free dispatch", next)
	}
	interval, _ := s.failed(key, a, false, now)
	if interval != time.Second {
		t.Fatalf("first failure: wait %s, want 1s", interval)
	}
	// Doubling up to five minutes, one probe at a time, the oldest request first.
	for _, want := range []time.Duration{2, 4, 8, 16, 32, 64, 128, 256, 300, 300} {
		if next := s.admit(key, b, now); next.dispatch {
			t.Fatalf("a newer request got ahead of the waiting one: %+v", next)
		}
		if next := s.admit(key, a, now); next.dispatch || next.wait != interval {
			t.Fatalf("before the wait: %+v, want to wait %s", next, interval)
		}
		now = now.Add(interval)
		if next := s.admit(key, a, now); !next.dispatch || !next.probe {
			t.Fatalf("after the wait: %+v, want the probe", next)
		}
		if next := s.admit(key, b, now); next.dispatch {
			t.Fatalf("a second request went out while the probe is out: %+v", next)
		}
		interval, _ = s.failed(key, a, true, now)
		if interval != want*time.Second {
			t.Fatalf("refused probe: wait %s, want %s", interval, want*time.Second)
		}
	}
	// Each answered probe halves the wait; below a second the gate opens.
	for _, want := range []time.Duration{150 * time.Second, 75 * time.Second, 37500 * time.Millisecond, 18750 * time.Millisecond, 9375 * time.Millisecond, 4687500 * time.Microsecond, 2343750 * time.Microsecond, 1171875 * time.Microsecond, 0} {
		now = now.Add(interval)
		if next := s.admit(key, a, now); !next.dispatch || !next.probe {
			t.Fatalf("after the wait: %+v, want the probe", next)
		}
		interval, _ = s.answered(key, true, now)
		if interval != want {
			t.Fatalf("answered probe: wait %s, want %s", interval, want)
		}
		s.failed(key, a, false, now) // put a back in line for the next round
		if want == 0 {
			break
		}
	}
}

func TestOutageGateRequestsSentBeforeItClosedDoNotDoubleTheWait(t *testing.T) {
	s := &outageGates{gates: map[string]*outageGate{}}
	const key = "auth\x00model"
	now := time.Unix(0, 0)
	a, b, c := s.newTicket(), s.newTicket(), s.newTicket()
	for _, ticket := range []uint64{a, b, c} {
		if next := s.admit(key, ticket, now); !next.dispatch {
			t.Fatalf("open gate held a request: %+v", next)
		}
	}
	for _, ticket := range []uint64{a, b, c} {
		if interval, _ := s.failed(key, ticket, false, now); interval != time.Second {
			t.Fatalf("failure of a request sent while open: wait %s, want 1s", interval)
		}
	}
	if _, waiting := s.answered(key, false, now); waiting != 3 {
		t.Fatalf("%d waiting, want 3", waiting)
	}
}

func TestOutageGateAnsweredProbeOpensAndReleasesEveryone(t *testing.T) {
	s := &outageGates{gates: map[string]*outageGate{}}
	const key = "auth\x00model"
	now := time.Unix(0, 0)
	a, b := s.newTicket(), s.newTicket()
	s.admit(key, a, now)
	s.failed(key, a, false, now) // closed for 1s
	s.admit(key, b, now)         // b waits behind a
	now = now.Add(time.Second)
	if next := s.admit(key, a, now); !next.probe {
		t.Fatalf("want a as the probe: %+v", next)
	}
	if interval, _ := s.answered(key, true, now); interval != 0 {
		t.Fatalf("a probe answered after a 1s wait: wait %s, want the gate open", interval)
	}
	if next := s.admit(key, b, now); !next.dispatch || next.probe {
		t.Fatalf("open gate: %+v, want b to go", next)
	}
}

func TestIsOutageError(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{outageTestError{http.StatusServiceUnavailable, "No capacity available"}, true},
		{outageTestError{http.StatusInternalServerError, "internal"}, true},
		{outageTestError{http.StatusRequestTimeout, "timeout"}, true},
		{outageTestError{http.StatusTooManyRequests, `{"reason":"MODEL_CAPACITY_EXHAUSTED"}`}, true},
		{outageTestError{http.StatusTooManyRequests, `{"reason":"QUOTA_EXHAUSTED"}`}, false},
		{outageTestError{http.StatusBadRequest, "invalid argument"}, false},
		{outageTestError{http.StatusUnauthorized, "expired"}, false},
		{&url.Error{Op: "Post", URL: "https://example.com", Err: io.ErrUnexpectedEOF}, true},
		{errors.New("translator: bad payload"), false},
	}
	for _, tc := range cases {
		if got := isOutageError(tc.err); got != tc.want {
			t.Errorf("isOutageError(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestOutageBackoffsListsGatesThatAreNotOpen(t *testing.T) {
	m := NewManager(nil, nil, nil)
	auth := &Auth{ID: "backoff-view-auth", Provider: "antigravity", Status: StatusActive}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register: %v", errRegister)
	}
	s := m.outageGates()
	now := time.Unix(1000, 0)
	open, closed := auth.ID+"\x00gemini-3.8-flash", auth.ID+"\x00gemini-4-neon"
	a, b := s.newTicket(), s.newTicket()
	s.admit(open, a, now)
	s.admit(closed, b, now)
	s.failed(closed, b, false, now)
	s.admit(closed, s.newTicket(), now)

	views := m.OutageBackoffs(now.Add(400 * time.Millisecond))
	if len(views) != 1 {
		t.Fatalf("views = %+v, want only the closed gate", views)
	}
	want := OutageBackoffView{AuthIndex: auth.EnsureIndex(), Model: "gemini-4-neon", IntervalMs: 1000, NextRetryInMs: 600, Waiting: 2}
	if views[0] != want {
		t.Fatalf("view = %+v, want %+v", views[0], want)
	}
}

func TestOutageBackoffsLeavesOutAGateThatHoldsNothingBack(t *testing.T) {
	m := NewManager(nil, nil, nil)
	s := m.outageGates()
	now := time.Unix(1000, 0)
	key, ticket := "auth\x00gemini-4-neon", s.newTicket()
	s.admit(key, ticket, now)
	s.failed(key, ticket, false, now)
	s.left(key, ticket, false) // the only waiting request went away
	if views := m.OutageBackoffs(now.Add(500 * time.Millisecond)); len(views) != 1 {
		t.Fatalf("views = %+v, want the gate while its try is due", views)
	}
	if views := m.OutageBackoffs(now.Add(2 * time.Second)); len(views) != 0 {
		t.Fatalf("views = %+v, want none once nobody waits and no try is due", views)
	}
}
