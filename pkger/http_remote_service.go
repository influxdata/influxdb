package pkger

import (
	"context"
	"net/http"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/pkg/httpc"
)

// HTTPRemoteService provides an http client that is fluent in all things pkger.
type HTTPRemoteService struct {
	Client *httpc.Client
}

var _ SVC = (*HTTPRemoteService)(nil)

// CreatePkg will produce a pkg from the parameters provided.
func (s *HTTPRemoteService) CreatePkg(ctx context.Context, setters ...CreatePkgSetFn) (*Pkg, error) {
	var opt CreateOpt
	for _, setter := range setters {
		if err := setter(&opt); err != nil {
			return nil, err
		}
	}

	var orgIDs []ReqCreateOrgIDOpt
	for _, org := range opt.OrgIDs {
		orgIDs = append(orgIDs, ReqCreateOrgIDOpt{
			OrgID: org.OrgID.String(),
			Filters: struct {
				ByLabel        []string `json:"byLabel"`
				ByResourceKind []Kind   `json:"byResourceKind"`
			}{
				ByLabel:        org.LabelNames,
				ByResourceKind: org.ResourceKinds,
			},
		})
	}

	reqBody := ReqCreatePkg{
		OrgIDs:    orgIDs,
		Resources: opt.Resources,
	}

	var newPkg *Pkg
	err := s.Client.
		PostJSON(reqBody, RoutePrefix).
		Decode(func(resp *http.Response) error {
			pkg, err := Parse(EncodingJSON, FromReader(resp.Body))
			newPkg = pkg
			return err
		}).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	if err := newPkg.Validate(ValidWithoutResources()); err != nil {
		return nil, err
	}
	return newPkg, nil
}

// DryRun provides a dry run of the pkg application. The pkg will be marked verified
// for later calls to Apply. This func will be run on an Apply if it has not been run
// already.
func (s *HTTPRemoteService) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (Summary, Diff, error) {
	return s.apply(ctx, orgID, pkg, true, opts...)
}

// Apply will apply all the resources identified in the provided pkg. The entire pkg will be applied
// in its entirety. If a failure happens midway then the entire pkg will be rolled back to the state
// from before the pkg was applied.
func (s *HTTPRemoteService) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (Summary, error) {
	sum, _, err := s.apply(ctx, orgID, pkg, false, opts...)
	return sum, err
}

func (s *HTTPRemoteService) apply(ctx context.Context, orgID influxdb.ID, pkg *Pkg, dryRun bool, opts ...ApplyOptFn) (Summary, Diff, error) {
	var opt ApplyOpt
	for _, o := range opts {
		if err := o(&opt); err != nil {
			return Summary{}, Diff{}, err
		}
	}

	b, err := pkg.Encode(EncodingJSON)
	if err != nil {
		return Summary{}, Diff{}, err
	}

	reqBody := ReqApplyPkg{
		OrgID:   orgID.String(),
		DryRun:  dryRun,
		EnvRefs: opt.EnvRefs,
		Secrets: opt.MissingSecrets,
		RawPkg:  b,
	}

	var resp RespApplyPkg
	err = s.Client.
		PostJSON(reqBody, RoutePrefix, "/apply").
		DecodeJSON(&resp).
		Do(ctx)
	if err != nil {
		return Summary{}, Diff{}, err
	}

	return resp.Summary, resp.Diff, NewParseError(resp.Errors...)
}
