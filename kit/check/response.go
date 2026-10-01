package check

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
)

// Response is the result of a single health check.
//
// Implementations derive Status() and Message() at the moment they are
// called, so a stateful implementation (e.g. FreshnessResponse) can
// flip a previously-passing response to fail when its cached snapshot
// becomes stale. Every implementation marshals to the same wire shape:
// {"name","status","message"?,"checks"?,"<group>"?...}, one key per
// Measures group. BasicResponse renders it in MarshalJSON;
// FreshnessResponse and renamedResponse render through a BasicResponse
// snapshot.
//
// A stateful implementation should also implement HealthSnapshotter, so callers
// that need every field from one observation -- rendering, or freezing a
// terminal report -- can take it without reading each accessor separately.
type Response interface {
	Name() string
	Status() Status
	Message() string
	Checks() Responses
	// Measures returns the numbers the check reports beside its message,
	// grouped by unit. The result is shared and must not be modified.
	Measures() Measures
}

// HealthSnapshotter is implemented by a Response that can render its entire state
// from a single coherent read. A Response whose fields derive from mutable
// state -- FreshnessResponse -- can otherwise report a torn combination, a
// status taken from one observation and a message from the next, because the
// four accessors each read independently.
//
// Implementations must return a BasicResponse whose own fields are fixed;
// nested Checks may still be live, and snapshot recurses into them.
type HealthSnapshotter interface {
	Snapshot() BasicResponse
}

// snapshot flattens r into a value whose fields are fixed at the moment of the
// call, using Snapshot when r implements it and the four accessors otherwise,
// and recursing into nested checks so no live Response survives inside the
// result.
//
// Flattening matters wherever a Response outlives the thing it describes: a
// *FreshnessResponse held by pointer goes on aging into a staleness failure
// after its prober stops, so a set of them kept as-is would drift. Every
// Response marshals to the same wire shape, so the flattened value renders
// identical JSON to the one it replaces.
func snapshot(r Response) BasicResponse {
	if s, ok := r.(HealthSnapshotter); ok {
		b := s.Snapshot()
		return NewBasicResponse(b.Name(), b.Status(), b.Message(), snapshotAll(b.Checks())).
			withMeasures(b.Measures())
	}
	return NewBasicResponse(r.Name(), r.Status(), r.Message(), snapshotAll(r.Checks())).
		withMeasures(r.Measures())
}

// snapshotAll flattens every element of rs. It returns nil for an empty input
// so the result keeps the shape omitempty gives Checks: an absent field rather
// than "checks":[].
func snapshotAll(rs Responses) Responses {
	if len(rs) == 0 {
		return nil
	}
	out := make(Responses, len(rs))
	for i, r := range rs {
		out[i] = snapshot(r)
	}
	return out
}

// wireResponse is the fixed part of the on-the-wire JSON shape shared by
// every Response implementation, used for both marshal and unmarshal.
// Field tags MUST match the legacy struct format so external clients of
// /health and /ready see identical bytes. Decoding into the Checks field
// relies on Responses.UnmarshalJSON to allocate the concrete element type.
// Measure groups are not here: their keys vary, so BasicResponse's
// MarshalJSON and UnmarshalJSON handle them around this struct.
type wireResponse struct {
	Name    string    `json:"name"`
	Status  Status    `json:"status"`
	Message string    `json:"message,omitempty"`
	Checks  Responses `json:"checks,omitempty"`
}

// BasicResponse is the Response implementation. Construct via
// Pass/Info/Error/Fail/NamedPass/NamedFail/NewBasicResponse; the zero
// value has empty Name/Status/Message and is not safe to return from a
// Checker — its wire status would be the empty string, which the HTTP
// handler treats as pass.
//
// measures is never modified after construction: WithMeasure builds a new
// map, so copies of a BasicResponse may share it, including across the
// goroutines that serve a frozen response.
type BasicResponse struct {
	wireResponse
	measures Measures
}

// NewBasicResponse builds a BasicResponse with every field set except
// measures, which WithMeasure adds. Used by CheckHealth/CheckReady to
// build the aggregate response. Rebuilding a response through here is
// also how the HTTP handler withholds detail: what it does not copy over,
// measures included, is not served.
func NewBasicResponse(name string, status Status, message string, checks Responses) BasicResponse {
	return BasicResponse{wireResponse: wireResponse{Name: name, Status: status, Message: message, Checks: checks}}
}

func (b BasicResponse) Name() string       { return b.wireResponse.Name }
func (b BasicResponse) Status() Status     { return b.wireResponse.Status }
func (b BasicResponse) Message() string    { return b.wireResponse.Message }
func (b BasicResponse) Checks() Responses  { return b.wireResponse.Checks }
func (b BasicResponse) Measures() Measures { return b.measures }

// WithName returns a copy of b with the given name. Used by namedChecker
// and startupChecker to override the inner checker's name without an
// extra wrapper.
func (b BasicResponse) WithName(name string) BasicResponse {
	b.wireResponse.Name = name
	return b
}

// WithMeasure returns a copy of b reporting m under group, replacing any
// group of that name. m is copied, so the caller may reuse its map.
//
// Invalid input is dropped rather than reported, because a check must
// always render: group, m.Unit and every value key must be non-empty runs
// of [a-z0-9_]; group may not be one of the check's own fields (name,
// status, message, checks); no value key may be "unit"; and non-finite
// values are discarded. If no value survives, b is returned unchanged.
func (b BasicResponse) WithMeasure(group string, m Measure) BasicResponse {
	return b.withMeasures(Measures{group: m})
}

// withMeasures returns a copy of b with every group of ms added through
// cleanMeasure, the same rules WithMeasure applies. It is how a response
// rebuilt from another Response's accessors keeps that response's groups,
// without trusting the other implementation to have validated them.
func (b BasicResponse) withMeasures(ms Measures) BasicResponse {
	if len(ms) == 0 {
		return b
	}
	out := maps.Clone(b.measures)
	for group, m := range ms {
		c, ok := cleanMeasure(group, m)
		if !ok {
			continue
		}
		if out == nil {
			out = make(Measures, len(ms))
		}
		out[group] = c
	}
	b.measures = out
	return b
}

// MarshalJSON renders the fixed fields exactly as wireResponse always has,
// then one member per measure group, in sorted group order. With no groups
// the bytes are those of wireResponse alone, so a check that reports no
// numbers renders as it did before measures existed.
func (b BasicResponse) MarshalJSON() ([]byte, error) {
	base, err := json.Marshal(b.wireResponse)
	if err != nil {
		return nil, err
	}
	if len(b.measures) == 0 {
		return base, nil
	}
	// base is a JSON object with at least name and status, so the closing
	// brace is its last byte and a comma always separates what follows.
	buf := bytes.NewBuffer(base[:len(base)-1])
	for _, group := range slices.Sorted(maps.Keys(b.measures)) {
		buf.WriteByte(',')
		if err := writeJSONMember(buf, group, b.measures[group]); err != nil {
			return nil, fmt.Errorf("marshaling measure group %q of check %q: %w", group, b.wireResponse.Name, err)
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// UnmarshalJSON decodes the fixed fields into wireResponse, then takes as a
// measure group every other member that decodes as a Measure. Members that
// are not measure-shaped are ignored, as encoding/json ignores unknown
// fields: QueryHealthCheck decodes a whole /health body into a
// BasicResponse, and its version and commit strings must not fail that.
func (b *BasicResponse) UnmarshalJSON(data []byte) error {
	var w wireResponse
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		return err
	}
	var ms Measures
	for key, raw := range members {
		if isReservedField(key) {
			continue
		}
		var m Measure
		if json.Unmarshal(raw, &m) != nil {
			continue
		}
		if ms == nil {
			ms = make(Measures)
		}
		ms[key] = m
	}
	*b = BasicResponse{wireResponse: w}.withMeasures(ms)
	return nil
}

// HasCheck reports whether r contains a sub-check with the given name.
func HasCheck(r Response, name string) bool {
	for _, c := range r.Checks() {
		if c.Name() == name {
			return true
		}
	}
	return false
}

// Responses is a sortable collection of Response objects.
type Responses []Response

// UnmarshalJSON decodes a JSON array of check objects, allocating a
// BasicResponse for each element and lifting them into the Response
// interface slice. encoding/json cannot do this directly because
// []Response is a slice of interface values with no concrete type.
func (r *Responses) UnmarshalJSON(data []byte) error {
	var basics []BasicResponse
	if err := json.Unmarshal(data, &basics); err != nil {
		return err
	}
	if len(basics) == 0 {
		*r = nil
		return nil
	}
	out := make(Responses, len(basics))
	for i := range basics {
		out[i] = basics[i]
	}
	*r = out
	return nil
}

func (r Responses) Len() int { return len(r) }

// Less defines the order in which responses are sorted.
//
// Failing responses are always sorted before passing responses. Responses with
// the same status are then sorted according to the name of the check.
func (r Responses) Less(i, j int) bool {
	si, sj := r[i].Status(), r[j].Status()
	if si == sj {
		return r[i].Name() < r[j].Name()
	}
	return si < sj
}

func (r Responses) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
