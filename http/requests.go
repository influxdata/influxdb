package http

import (
	"context"
	"net/http"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	platform "github.com/influxdata/influxdb/v2"
)

const (
	// OrgID is the http query parameter to specify an organization by ID.
	OrgID = "orgID"
	// Org is the http query parameter that take either the ID or Name interchangeably
	Org = "org"
	// BucketID is the http query parameter to specify an bucket by ID.
	BucketID = "bucketID"
	// Bucket is the http query parameter take either the ID or Name interchangably
	Bucket = "bucket"
)

// queryOrganization returns the organization for any http request.
//
// It checks the org= and then orgID= parameter of the request.
//
// This will try to find the organization using an ID string or
// the name.  It interprets the &org= parameter as either the name
// or the ID.
func queryOrganization(ctx context.Context, r *http.Request, svc platform.OrganizationService) (o *platform.Organization, err error) {
	filter := platform.OrganizationFilter{}
	if organization := r.URL.Query().Get(Org); organization != "" {
		if id, err := platform2.IDFromString(organization); err == nil {
			filter.ID = id
		} else {
			filter.Name = &organization
		}
	}

	if reqID := r.URL.Query().Get(OrgID); reqID != "" {
		filter.ID, err = platform2.IDFromString(reqID)
		if err != nil {
			return nil, err
		}
	}
	return svc.FindOrganization(ctx, filter)
}

// queryBucket returns the bucket for any http request.
//
// It checks the bucket= and then bucketID= parameter of the request.
//
// This will try to find the bucket using an ID string or
// the name.  It interprets the &bucket= parameter as either the name
// or the ID.
func queryBucket(ctx context.Context, orgID platform2.ID, r *http.Request, svc platform.BucketService) (b *platform.Bucket, err error) {
	filter := platform.BucketFilter{OrganizationID: &orgID}
	if bucket := r.URL.Query().Get(Bucket); bucket != "" {
		if id, err := platform2.IDFromString(bucket); err == nil {
			filter.ID = id
		} else {
			filter.Name = &bucket
		}
	}
	if reqID := r.URL.Query().Get(BucketID); reqID != "" {
		filter.ID, err = platform2.IDFromString(reqID)
		if err != nil {
			return nil, err
		}
	}
	if filter.ID == nil && filter.Name == nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Please provide either bucketID or bucket",
		}
	}
	return svc.FindBucket(ctx, filter)
}
