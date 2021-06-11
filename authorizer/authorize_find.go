package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

// AuthorizeFindDBRPs takes the given items and returns only the ones that the user is authorized to access.
func AuthorizeFindDBRPs(ctx context.Context, rs []*influxdb.DBRPMappingV2) ([]*influxdb.DBRPMappingV2, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		// N.B. we have to check both read and write permissions here to support the legacy write-path,
		// which calls AuthorizeFindDBRPs when locating the bucket underlying a DBRP target.
		_, _, err := AuthorizeRead(ctx, influxdb.BucketsResourceType, r.BucketID, r.OrganizationID)
		if err != nil {
			_, _, err = AuthorizeWrite(ctx, influxdb.BucketsResourceType, r.BucketID, r.OrganizationID)
		}
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindAuthorizations takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindAuthorizations(ctx context.Context, rs []*influxdb.Authorization) ([]*influxdb.Authorization, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.AuthorizationsResourceType, r.ID, r.OrgID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		_, _, err = AuthorizeReadResource(ctx, influxdb.UsersResourceType, r.UserID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindBuckets takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindBuckets(ctx context.Context, rs []*influxdb.Bucket) ([]*influxdb.Bucket, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeReadBucket(ctx, r.Type, r.ID, r.OrgID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindDashboards takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindDashboards(ctx context.Context, rs []*influxdb.Dashboard) ([]*influxdb.Dashboard, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.DashboardsResourceType, r.ID, r.OrganizationID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindAnnotations takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindAnnotations(ctx context.Context, rs []influxdb.StoredAnnotation) ([]influxdb.StoredAnnotation, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.AnnotationsResourceType, r.ID, r.OrgID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindStreams takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindStreams(ctx context.Context, rs []influxdb.StoredStream) ([]influxdb.StoredStream, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.AnnotationsResourceType, r.ID, r.OrgID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindOrganizations takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindOrganizations(ctx context.Context, rs []*influxdb.Organization) ([]*influxdb.Organization, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeReadOrg(ctx, r.ID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindSources takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindSources(ctx context.Context, rs []*influxdb.Source) ([]*influxdb.Source, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.SourcesResourceType, r.ID, r.OrganizationID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindTasks takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindTasks(ctx context.Context, rs []*taskmodel.Task) ([]*taskmodel.Task, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.TasksResourceType, r.ID, r.OrganizationID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindTelegrafs takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindTelegrafs(ctx context.Context, rs []*influxdb.TelegrafConfig) ([]*influxdb.TelegrafConfig, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.TelegrafsResourceType, r.ID, r.OrgID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindUsers takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindUsers(ctx context.Context, rs []*influxdb.User) ([]*influxdb.User, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeReadResource(ctx, influxdb.UsersResourceType, r.ID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindVariables takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindVariables(ctx context.Context, rs []*influxdb.Variable) ([]*influxdb.Variable, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.VariablesResourceType, r.ID, r.OrganizationID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindScrapers takes the given items and returns only the ones that the user is authorize to read.
func AuthorizeFindScrapers(ctx context.Context, rs []influxdb.ScraperTarget) ([]influxdb.ScraperTarget, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.ScraperResourceType, r.ID, r.OrgID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindLabels takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindLabels(ctx context.Context, rs []*influxdb.Label) ([]*influxdb.Label, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.LabelsResourceType, r.ID, r.OrgID)
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindNotificationRules takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindNotificationRules(ctx context.Context, rs []influxdb.NotificationRule) ([]influxdb.NotificationRule, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.NotificationRuleResourceType, r.GetID(), r.GetOrgID())
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindNotificationEndpoints takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindNotificationEndpoints(ctx context.Context, rs []influxdb.NotificationEndpoint) ([]influxdb.NotificationEndpoint, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.NotificationEndpointResourceType, r.GetID(), r.GetOrgID())
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindChecks takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindChecks(ctx context.Context, rs []influxdb.Check) ([]influxdb.Check, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		_, _, err := AuthorizeRead(ctx, influxdb.ChecksResourceType, r.GetID(), r.GetOrgID())
		if err != nil && errors.ErrorCode(err) != errors.EUnauthorized {
			return nil, 0, err
		}
		if errors.ErrorCode(err) == errors.EUnauthorized {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}

// AuthorizeFindUserResourceMappings takes the given items and returns only the ones that the user is authorized to read.
func AuthorizeFindUserResourceMappings(ctx context.Context, os OrgIDResolver, rs []*influxdb.UserResourceMapping) ([]*influxdb.UserResourceMapping, int, error) {
	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rrs := rs[:0]
	for _, r := range rs {
		orgID, err := os.FindResourceOrganizationID(ctx, r.ResourceType, r.ResourceID)
		if err != nil {
			return nil, 0, err
		}
		if _, _, err := AuthorizeRead(ctx, r.ResourceType, r.ResourceID, orgID); err != nil {
			continue
		}
		rrs = append(rrs, r)
	}
	return rrs, len(rrs), nil
}
