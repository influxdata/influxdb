package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/csv"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

const (
	queryPath = "/v1/query"

	statsTrailer = "Influx-Query-Statistics"
)

type QueryHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	csvEncoder query.MultiResultEncoder

	QueryService        query.QueryService
	OrganizationService platform.OrganizationService
}

// NewQueryHandler returns a new instance of QueryHandler.
func NewQueryHandler() *QueryHandler {
	h := &QueryHandler{
		Router:     httprouter.New(),
		csvEncoder: csv.NewMultiResultEncoder(csv.DefaultEncoderConfig()),
	}

	h.HandlerFunc("GET", "/ping", h.handlePing)
	h.HandlerFunc("POST", queryPath, h.handlePostQuery)
	return h
}

// handlePing returns a simple response to let the client know the server is running.
func (h *QueryHandler) handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
}

// handlePostQuery is the HTTP handler for the POST /v1/query route.
func (h *QueryHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var orgID platform.ID
	if id := r.FormValue("orgID"); id != "" {
		err := orgID.DecodeFromString(id)
		if err != nil {
			EncodeError(ctx, errors.Wrap(err, "failed to decode orgID", errors.MalformedData), w)
			return
		}
	}
	if name := r.FormValue("orgName"); name != "" {
		org, err := h.OrganizationService.FindOrganization(ctx, platform.OrganizationFilter{
			Name: &name,
		})
		if err != nil {
			EncodeError(ctx, errors.Wrap(err, "failed to load organization", errors.MalformedData), w)
			return
		}
		orgID = org.ID
	}

	if len(orgID) == 0 {
		EncodeError(ctx, errors.New("must pass organization name or ID as string in orgName or orgID parameter", errors.MalformedData), w)
		return
	}

	var results query.ResultIterator
	if r.Header.Get("Content-type") == "application/json" {
		req, err := decodePostQueryRequest(ctx, r)
		if err != nil {
			EncodeError(ctx, errors.Wrap(err, "Failed to decode query request", errors.MalformedData), w)
			return
		}

		rs, err := h.QueryService.Query(ctx, orgID, req.Spec)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}
		results = rs
	} else {
		queryStr := r.FormValue("q")
		if queryStr == "" {
			EncodeError(ctx, errors.New("must pass query string in q parameter", errors.MalformedData), w)
			return
		}
		rs, err := h.QueryService.QueryWithCompile(ctx, orgID, queryStr)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}
		results = rs
	}

	// Setup headers
	stats, hasStats := results.(query.Statisticser)
	if hasStats {
		w.Header().Set("Trailer", statsTrailer)
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Transfer-Encoding", "chunked")
	// NOTE: We do not write the headers here.
	// It is possible that if the encoding step fails
	// that we can write an error header so long as
	// the encoder did not write anything.
	// As such we rely on the http.ResponseWriter behavior
	// to write an StatusOK header with the first write.

	switch r.Header.Get("Accept") {
	case "text/csv":
		fallthrough
	default:
		n, err := h.csvEncoder.Encode(w, results)
		if err != nil {
			if n == 0 {
				// If the encoder did not write anything, we can write an error header.
				EncodeError(ctx, err, w)
			} else {
				h.Logger.Info("Failed to encode client response",
					zap.Error(err),
				)
			}
		}
	}

	if hasStats {
		data, err := json.Marshal(stats.Statistics())
		if err != nil {
			h.Logger.Info("Failed to encode statistics", zap.Error(err))
			return
		}
		// Write statisitcs trailer
		w.Header().Set(statsTrailer, string(data))
	}
}

type postQueryRequest struct {
	Spec *query.Spec `json:"spec"`
}

func decodePostQueryRequest(ctx context.Context, r *http.Request) (*postQueryRequest, error) {
	s := new(query.Spec)
	if err := json.NewDecoder(r.Body).Decode(s); err != nil {
		return nil, err
	}

	return &postQueryRequest{
		Spec: s,
	}, nil
}

type QueryService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

func (s *QueryService) Query(ctx context.Context, orgID platform.ID, query *query.Spec) (query.ResultIterator, error) {
	u, err := newURL(s.Addr, queryPath)
	if err != nil {
		return nil, err
	}
	values := url.Values{}
	values.Set("orgID", orgID.String())
	u.RawQuery = values.Encode()

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", s.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/csv")

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	return s.processResponse(resp)
}

func (s *QueryService) QueryWithCompile(ctx context.Context, orgID platform.ID, query string) (query.ResultIterator, error) {
	u, err := newURL(s.Addr, queryPath)
	if err != nil {
		return nil, err
	}
	values := url.Values{}
	values.Set("q", query)
	values.Set("orgID", orgID.String())
	u.RawQuery = values.Encode()

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", s.Token)
	req.Header.Set("Accept", "text/csv")

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	return s.processResponse(resp)
}

func (s *QueryService) processResponse(resp *http.Response) (query.ResultIterator, error) {
	if err := CheckError(resp); err != nil {
		return nil, err
	}

	// TODO(jsternberg): Handle a 204 response?

	var decoder query.MultiResultDecoder
	switch resp.Header.Get("Content-Type") {
	case "text/csv":
		fallthrough
	default:
		decoder = csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	}
	result, err := decoder.Decode(resp.Body)
	if err != nil {
		return nil, err
	}
	return &statsResultIterator{
		result: result,
		resp:   resp,
	}, nil
}

// statsResultIterator implements query.ResultIterator and query.Statisticser by reading the HTTP trailers.
type statsResultIterator struct {
	result     query.ResultIterator
	resp       *http.Response
	statisitcs query.Statistics
	err        error
}

func (s *statsResultIterator) More() bool {
	more := s.result.More()
	if !more {
		s.readStats()
	}
	return more
}

func (s *statsResultIterator) Next() query.Result {
	return s.result.Next()
}

func (s *statsResultIterator) Cancel() {
	s.result.Cancel()
	s.readStats()
}

func (s *statsResultIterator) Err() error {
	err := s.result.Err()
	if err != nil {
		return err
	}
	return s.err
}

func (s *statsResultIterator) Statistics() query.Statistics {
	return s.statisitcs
}

// readStats reads the query statisitcs off the response trailers.
func (s *statsResultIterator) readStats() {
	data := s.resp.Trailer.Get(statsTrailer)
	if data != "" {
		s.err = json.Unmarshal([]byte(data), &s.statisitcs)
	}
}
