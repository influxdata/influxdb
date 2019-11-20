package backend_test

import (
	"context"
	"errors"

	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
)

type runRecorder struct {
	isLegacyOrg func(influxdb.ID) bool
	call        recordCall
}

type recordCall struct {
	orgID    influxdb.ID
	org      string
	bucketID influxdb.ID
	bucket   string
	run      *influxdb.Run
}

func (r *runRecorder) Record(ctx context.Context, orgID influxdb.ID, org string, bucketID influxdb.ID, bucket string, run *influxdb.Run) error {
	auth, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return err
	}

	var expOrg *influxdb.ID
	if !r.isLegacyOrg(orgID) {
		expOrg = &orgID
	}

	if !auth.Allowed(influxdb.Permission{
		Action: influxdb.WriteAction,
		Resource: influxdb.Resource{
			Type:  influxdb.BucketsResourceType,
			ID:    &bucketID,
			OrgID: expOrg,
		},
	}) {
		return errors.New("permission not allowed")
	}

	r.call = recordCall{orgID, org, bucketID, bucket, run}

	return nil
}
