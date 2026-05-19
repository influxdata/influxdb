package check

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

// freshnessSnapshot is one observation written by a probe goroutine.
//
// at MUST retain its monotonic reading; do not Round(0) or UTC() it.
// FreshnessResponse uses time.Since(at) to derive staleness, and a
// stripped monotonic reading would expose the calculation to wall-clock
// jumps (NTP step, manual time change) and produce nonsense ages.
type freshnessSnapshot struct {
	resp Response
	at   time.Time
}

// FreshnessResponse is a Response implementation that ages out: callers
// (typically a background prober goroutine) push an underlying Response
// via Update; if no Update arrives within the configured staleness
// budget, Status() flips to StatusFail and Message() reports how long
// it has been since the last probe.
//
// FreshnessResponse never copies safely; pass it by pointer.
type FreshnessResponse struct {
	name      string
	staleness time.Duration
	snap      atomic.Pointer[freshnessSnapshot]
}

// NewFreshnessResponse returns an empty FreshnessResponse with the given
// name and staleness budget. Until Update is first called, Status()
// returns StatusFail and Message() reports "no probe completed yet".
func NewFreshnessResponse(name string, staleness time.Duration) *FreshnessResponse {
	return &FreshnessResponse{name: name, staleness: staleness}
}

// Update records r as the latest probe result with the current
// monotonic timestamp. Safe to call concurrently with Name/Status/
// Message/Checks/MarshalJSON.
func (f *FreshnessResponse) Update(r Response) {
	f.snap.Store(&freshnessSnapshot{resp: r, at: time.Now()})
}

// Name returns the configured name.
func (f *FreshnessResponse) Name() string { return f.name }

// Status returns StatusFail when no probe has run yet or the last
// probe is older than the staleness budget; otherwise it returns the
// underlying probe's Status.
func (f *FreshnessResponse) Status() Status {
	s := f.snap.Load()
	if s == nil || time.Since(s.at) > f.staleness {
		return StatusFail
	}
	return s.resp.Status()
}

// Message returns a "no probe" / "stale" message when the snapshot is
// missing or aged out; otherwise it returns the underlying probe's
// Message.
func (f *FreshnessResponse) Message() string {
	s := f.snap.Load()
	if s == nil {
		return "no probe completed yet"
	}
	if age := time.Since(s.at); age > f.staleness {
		return staleMessage(age, f.staleness)
	}
	return s.resp.Message()
}

// Checks returns the underlying probe's nested checks when fresh;
// otherwise nil.
func (f *FreshnessResponse) Checks() Responses {
	s := f.snap.Load()
	if s == nil || time.Since(s.at) > f.staleness {
		return nil
	}
	return s.resp.Checks()
}

// MarshalJSON emits a wireResponse derived from one atomic snapshot
// load. Reading every field through the four interface methods would
// be correct (each does its own atomic load) but could observe two
// different snapshots across the call sequence; a single load here
// guarantees the rendered JSON object reflects exactly one state.
func (f *FreshnessResponse) MarshalJSON() ([]byte, error) {
	w := wireResponse{Name: f.name}
	s := f.snap.Load()
	switch {
	case s == nil:
		w.Status = StatusFail
		w.Message = "no probe completed yet"
	default:
		if age := time.Since(s.at); age > f.staleness {
			w.Status = StatusFail
			w.Message = staleMessage(age, f.staleness)
		} else {
			w.Status = s.resp.Status()
			w.Message = s.resp.Message()
			w.Checks = s.resp.Checks()
		}
	}
	return json.Marshal(w)
}

func staleMessage(age, threshold time.Duration) string {
	return fmt.Sprintf("stale: last probe %s ago (threshold %s)",
		age.Round(time.Millisecond), threshold)
}
