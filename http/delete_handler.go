package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	http "net/http"
	"time"

	"github.com/influxdata/influxql"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/predicate"
	"go.uber.org/zap"
)

// DeleteBackend is all services and associated parameters required to construct
// the DeleteHandler.
type DeleteBackend struct {
	log *zap.Logger
	errors.HTTPErrorHandler

	DeleteService       influxdb.DeleteService
	BucketService       influxdb.BucketService
	OrganizationService influxdb.OrganizationService
}

// NewDeleteBackend returns a new instance of DeleteBackend
func NewDeleteBackend(log *zap.Logger, b *APIBackend) *DeleteBackend {
	return &DeleteBackend{
		log: log,

		HTTPErrorHandler:    b.HTTPErrorHandler,
		DeleteService:       b.DeleteService,
		BucketService:       b.BucketService,
		OrganizationService: b.OrganizationService,
	}
}

// DeleteHandler receives a delete request with a predicate and sends it to storage.
type DeleteHandler struct {
	errors.HTTPErrorHandler
	*httprouter.Router

	log *zap.Logger

	DeleteService       influxdb.DeleteService
	BucketService       influxdb.BucketService
	OrganizationService influxdb.OrganizationService
}

const (
	prefixDelete = "/api/v2/delete"
)

var (
	msgStartTooSoon = fmt.Sprintf("invalid start time, start time must not be before %s", time.Unix(0, models.MinNanoTime).UTC().Format(time.RFC3339Nano))
	msgStopTooLate  = fmt.Sprintf("invalid stop time, stop time must not be after %s", time.Unix(0, models.MaxNanoTime).UTC().Format(time.RFC3339Nano))
)

// NewDeleteHandler creates a new handler at /api/v2/delete to receive delete requests.
func NewDeleteHandler(log *zap.Logger, b *DeleteBackend) *DeleteHandler {
	h := &DeleteHandler{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Router:           NewRouter(b.HTTPErrorHandler),
		log:              log,

		BucketService:       b.BucketService,
		DeleteService:       b.DeleteService,
		OrganizationService: b.OrganizationService,
	}

	h.HandlerFunc("POST", prefixDelete, h.handleDelete)
	return h
}

func (h *DeleteHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "DeleteHandler")
	defer span.Finish()

	ctx := r.Context()
	defer r.Body.Close()

	a, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	dr, measurement, err := decodeDeleteRequest(
		ctx, r,
		h.OrganizationService,
		h.BucketService,
	)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	p, err := influxdb.NewPermissionAtID(dr.Bucket.ID, influxdb.WriteAction, influxdb.BucketsResourceType, dr.Org.ID)
	if err != nil {
		h.HandleHTTPError(ctx, &errors.Error{
			Code: errors.EInternal,
			Op:   "http/handleDelete",
			Msg:  fmt.Sprintf("unable to create permission for bucket: %v", err),
			Err:  err,
		}, w)
		return
	}

	if pset, err := a.PermissionSet(); err != nil || !pset.Allowed(*p) {
		h.HandleHTTPError(ctx, &errors.Error{
			Code: errors.EForbidden,
			Op:   "http/handleDelete",
			Msg:  "insufficient permissions to delete",
		}, w)
		return
	}

	if err := h.DeleteService.DeleteBucketRangePredicate(r.Context(), dr.Org.ID, dr.Bucket.ID, dr.Start, dr.Stop, dr.Predicate, measurement); err != nil {
		h.HandleHTTPError(ctx, &errors.Error{
			Code: errors.EInternal,
			Op:   "http/handleDelete",
			Msg:  fmt.Sprintf("unable to delete: %v", err),
			Err:  err,
		}, w)
		return
	}

	h.log.Debug("Deleted",
		zap.String("orgID", fmt.Sprint(dr.Org.ID.String())),
		zap.String("bucketID", fmt.Sprint(dr.Bucket.ID.String())),
	)

	w.WriteHeader(http.StatusNoContent)
}

func decodeDeleteRequest(ctx context.Context, r *http.Request, orgSvc influxdb.OrganizationService, bucketSvc influxdb.BucketService) (*deleteRequest, influxql.Expr, error) {
	dr := new(deleteRequest)
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		je := &errors.Error{
			Code: errors.EInvalid,
			Msg:  "error reading json body",
			Err:  err,
		}
		return nil, nil, je
	}
	buffer := bytes.NewBuffer(buf)
	err = json.NewDecoder(buffer).Decode(dr)
	if err != nil {
		je := &errors.Error{
			Code: errors.EInvalid,
			Msg:  "error decoding json body",
			Err:  err,
		}
		return nil, nil, je
	}

	var drd deleteRequestDecode
	err = json.Unmarshal(buf, &drd)
	if err != nil {
		je := &errors.Error{
			Code: errors.EInvalid,
			Msg:  "error decoding json body for predicate",
			Err:  err,
		}
		return nil, nil, je
	}
	var measurementExpr influxql.Expr
	if drd.Predicate != "" {
		expr, err := influxql.ParseExpr(drd.Predicate)
		if err != nil {
			return nil, nil, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "invalid request; error parsing predicate",
				Err:  err,
			}
		}
		measurementExpr, _, err = influxql.PartitionExpr(influxql.CloneExpr(expr), func(e influxql.Expr) (bool, error) {
			switch e := e.(type) {
			case *influxql.BinaryExpr:
				switch e.Op {
				case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
					tag, ok := e.LHS.(*influxql.VarRef)
					if ok && tag.Val == "_measurement" {
						return true, nil
					}
				}
			}
			return false, nil
		})

		var walkError error
		influxql.WalkFunc(expr, func(e influxql.Node) {
			if v, ok := e.(*influxql.BinaryExpr); ok {
				if vv, ok := v.LHS.(*influxql.VarRef); ok && v.Op == influxql.EQ {
					if vv.Val == "_field" {
						walkError = &errors.Error{
							Code: errors.ENotImplemented,
							Msg:  "",
							Err:  fmt.Errorf("delete by field is not supported"),
						}
					}
				}
			}
		})
		if walkError != nil {
			return nil, nil, walkError
		}
		if err != nil {
			return nil, nil, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "invalid request; error partitioning predicate",
				Err:  err,
			}
		}
	}

	if dr.Org, err = queryOrganization(ctx, r, orgSvc); err != nil {
		return nil, nil, err
	}

	if dr.Bucket, err = queryBucket(ctx, dr.Org.ID, r, bucketSvc); err != nil {
		return nil, nil, err
	}
	return dr, measurementExpr, nil
}

type deleteRequest struct {
	Org       *influxdb.Organization
	Bucket    *influxdb.Bucket
	Start     int64
	Stop      int64
	Predicate influxdb.Predicate
}

type deleteRequestDecode struct {
	Start     string `json:"start"`
	Stop      string `json:"stop"`
	Predicate string `json:"predicate"`
}

// DeleteRequest is the request send over http to delete points.
type DeleteRequest struct {
	OrgID     string `json:"-"`
	Org       string `json:"-"` // org name
	BucketID  string `json:"-"`
	Bucket    string `json:"-"`
	Start     string `json:"start"`
	Stop      string `json:"stop"`
	Predicate string `json:"predicate"`
}

func (dr *deleteRequest) UnmarshalJSON(b []byte) error {
	var drd deleteRequestDecode
	if err := json.Unmarshal(b, &drd); err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Invalid delete predicate node request",
			Err:  err,
		}
	}
	*dr = deleteRequest{}
	start, err := time.Parse(time.RFC3339Nano, drd.Start)
	if err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Op:   "http/Delete",
			Msg:  "invalid RFC3339Nano for field start, please format your time with RFC3339Nano format, example: 2009-01-02T23:00:00Z",
		}
	}
	if err = models.CheckTime(start); err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Op:   "http/Delete",
			Msg:  msgStartTooSoon,
		}
	}
	dr.Start = start.UnixNano()

	stop, err := time.Parse(time.RFC3339Nano, drd.Stop)
	if err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Op:   "http/Delete",
			Msg:  "invalid RFC3339Nano for field stop, please format your time with RFC3339Nano format, example: 2009-01-01T23:00:00Z",
		}
	}
	if err = models.CheckTime(stop); err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Op:   "http/Delete",
			Msg:  msgStopTooLate,
		}
	}
	dr.Stop = stop.UnixNano()
	node, err := predicate.Parse(drd.Predicate)
	if err != nil {
		return err
	}
	dr.Predicate, err = predicate.New(node)
	return err
}

// DeleteService sends data over HTTP to delete points.
type DeleteService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// DeleteBucketRangePredicate send delete request over http to delete points.
func (s *DeleteService) DeleteBucketRangePredicate(ctx context.Context, dr DeleteRequest) error {
	u, err := NewURL(s.Addr, prefixDelete)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(dr); err != nil {
		return err
	}
	req, err := http.NewRequest("POST", u.String(), buf)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	SetToken(s.Token, req)

	params := req.URL.Query()
	if dr.OrgID != "" {
		params.Set("orgID", dr.OrgID)
	} else if dr.Org != "" {
		params.Set("org", dr.Org)
	}

	if dr.BucketID != "" {
		params.Set("bucketID", dr.BucketID)
	} else if dr.Bucket != "" {
		params.Set("bucket", dr.Bucket)
	}
	req.URL.RawQuery = params.Encode()

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp)
}
