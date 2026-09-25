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

// msgNoProbe is what a FreshnessResponse reports before its first Update:
// the probe has not run yet, which is distinct from having run and aged out.
const msgNoProbe = "no probe completed yet"

// NewFreshnessResponse returns an empty FreshnessResponse with the given
// name and staleness budget. Until Update is first called, Status()
// returns StatusFail and Message() reports msgNoProbe.
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
		return msgNoProbe
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

// Snapshot renders f from one atomic load, implementing Snapshotter.
// Reading every field through the four interface methods would be
// correct (each does its own atomic load) but could observe two
// different snapshots across the call sequence, yielding a combination
// that was never true: a stale status carried alongside the previous
// probe's empty message, which /health renders as the bare word "fail".
//
// The returned BasicResponse fixes f's own fields. Its Checks are the
// underlying probe's, which this type does not own and does not copy.
func (f *FreshnessResponse) Snapshot() BasicResponse {
	s := f.snap.Load()
	if s == nil {
		return NewBasicResponse(f.name, StatusFail, msgNoProbe, nil)
	}
	if age := time.Since(s.at); age > f.staleness {
		return NewBasicResponse(f.name, StatusFail, staleMessage(age, f.staleness), nil)
	}
	if inner, ok := s.resp.(Snapshotter); ok {
		return inner.Snapshot().WithName(f.name)
	}
	return NewBasicResponse(f.name, s.resp.Status(), s.resp.Message(), s.resp.Checks())
}

// MarshalJSON emits the wire shape from a single snapshot, so the
// rendered JSON object reflects exactly one state. BasicResponse embeds
// wireResponse, whose exported fields encoding/json promotes, so this
// marshals byte-identically to building the wireResponse here.
func (f *FreshnessResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.Snapshot())
}

func staleMessage(age, threshold time.Duration) string {
	return fmt.Sprintf("stale: last probe %s ago (threshold %s)",
		age.Round(time.Millisecond), threshold)
}
