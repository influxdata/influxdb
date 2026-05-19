package check

import (
	"encoding/json"
)

// Response is the result of a single health check.
//
// Implementations derive Status() and Message() at the moment they are
// called, so a stateful implementation (e.g. FreshnessResponse) can
// flip a previously-passing response to fail when its cached snapshot
// becomes stale. Every implementation marshals to the same wire shape:
// {"name","status","message"?,"checks"?}. BasicResponse gets this for
// free by embedding wireResponse; FreshnessResponse and renamedResponse
// build the same shape explicitly in MarshalJSON.
type Response interface {
	Name() string
	Status() Status
	Message() string
	Checks() Responses
}

// wireResponse is the on-the-wire JSON shape shared by every Response
// implementation, used for both marshal and unmarshal. Field tags MUST
// match the legacy struct format so external clients of /health and
// /ready see identical bytes. Decoding into the Checks field relies on
// Responses.UnmarshalJSON to allocate the concrete element type.
type wireResponse struct {
	Name    string    `json:"name"`
	Status  Status    `json:"status"`
	Message string    `json:"message,omitempty"`
	Checks  Responses `json:"checks,omitempty"`
}

// BasicResponse is the Response implementation.
// Construct via Pass/Info/Error/Fail/NamedPass/NamedFail/NewBasicResponse;
// the zero value is a fail with empty name and message.
type BasicResponse struct {
	wireResponse
}

// NewBasicResponse builds a BasicResponse with every field set. Used by
// CheckHealth/CheckReady to build the aggregate response.
func NewBasicResponse(name string, status Status, message string, checks Responses) BasicResponse {
	return BasicResponse{wireResponse{Name: name, Status: status, Message: message, Checks: checks}}
}

func (b BasicResponse) Name() string      { return b.wireResponse.Name }
func (b BasicResponse) Status() Status    { return b.wireResponse.Status }
func (b BasicResponse) Message() string   { return b.wireResponse.Message }
func (b BasicResponse) Checks() Responses { return b.wireResponse.Checks }

// WithName returns a copy of b with the given name. Used by namedChecker
// and startupChecker to override the inner checker's name without an
// extra wrapper.
func (b BasicResponse) WithName(name string) BasicResponse {
	b.wireResponse.Name = name
	return b
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
