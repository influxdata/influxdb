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

	h.HandlerFunc("POST", queryPath, h.handlePostQuery)
	return h
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

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	switch r.Header.Get("Accept") {
	case "text/csv":
		fallthrough
	default:
		n, err := h.csvEncoder.Encode(w, results)
		if err != nil {
			if n == 0 {
				EncodeError(ctx, err, w)
				return
			}
			h.Logger.Info("Failed to encode client response",
				zap.Error(err),
			)
		}
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
	return decoder.Decode(resp.Body)
}
