package resource

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

// Resolver is a type which combines multiple resource services
// in order to resolve the resources associated org ID.
// Ideally you do not need to use this type, it is mostly a stop-gap
// while we migrate responsibilities off of *kv.Service.
// Consider it deprecated.
type Resolver struct {
	AuthorizationFinder interface {
		FindAuthorizationByID(context.Context, influxdb.ID) (*influxdb.Authorization, error)
	}
	BucketFinder interface {
		FindBucketByID(context.Context, influxdb.ID) (*influxdb.Bucket, error)
	}
	OrganizationFinder interface {
		FindOrganizationByID(context.Context, influxdb.ID) (*influxdb.Organization, error)
	}
	DashboardFinder interface {
		FindDashboardByID(context.Context, influxdb.ID) (*influxdb.Dashboard, error)
	}
	SourceFinder interface {
		FindSourceByID(context.Context, influxdb.ID) (*influxdb.Source, error)
	}
	TaskFinder interface {
		FindTaskByID(context.Context, influxdb.ID) (*influxdb.Task, error)
	}
	TelegrafConfigFinder interface {
		FindTelegrafConfigByID(context.Context, influxdb.ID) (*influxdb.TelegrafConfig, error)
	}
	VariableFinder interface {
		FindVariableByID(context.Context, influxdb.ID) (*influxdb.Variable, error)
	}
	TargetFinder interface {
		GetTargetByID(context.Context, influxdb.ID) (*influxdb.ScraperTarget, error)
	}
	CheckFinder interface {
		FindCheckByID(context.Context, influxdb.ID) (influxdb.Check, error)
	}
	NotificationEndpointFinder interface {
		FindNotificationEndpointByID(context.Context, influxdb.ID) (influxdb.NotificationEndpoint, error)
	}
	NotificationRuleFinder interface {
		FindNotificationRuleByID(context.Context, influxdb.ID) (influxdb.NotificationRule, error)
	}
}

// FindResourceOrganizationID is used to find the organization that a resource belongs to five the id of a resource and a resource type.
func (o *Resolver) FindResourceOrganizationID(ctx context.Context, rt influxdb.ResourceType, id influxdb.ID) (influxdb.ID, error) {
	switch rt {
	case influxdb.AuthorizationsResourceType:
		if o.AuthorizationFinder == nil {
			break
		}

		r, err := o.AuthorizationFinder.FindAuthorizationByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.OrgID, nil
	case influxdb.BucketsResourceType:
		if o.BucketFinder == nil {
			break
		}

		r, err := o.BucketFinder.FindBucketByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.OrgID, nil
	case influxdb.OrgsResourceType:
		if o.OrganizationFinder == nil {
			break
		}

		r, err := o.OrganizationFinder.FindOrganizationByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.ID, nil
	case influxdb.DashboardsResourceType:
		if o.DashboardFinder == nil {
			break
		}

		r, err := o.DashboardFinder.FindDashboardByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.OrganizationID, nil
	case influxdb.SourcesResourceType:
		if o.SourceFinder == nil {
			break
		}

		r, err := o.SourceFinder.FindSourceByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.OrganizationID, nil
	case influxdb.TasksResourceType:
		if o.TaskFinder == nil {
			break
		}

		r, err := o.TaskFinder.FindTaskByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.OrganizationID, nil
	case influxdb.TelegrafsResourceType:
		if o.TelegrafConfigFinder == nil {
			break
		}

		r, err := o.TelegrafConfigFinder.FindTelegrafConfigByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.OrgID, nil
	case influxdb.VariablesResourceType:
		if o.VariableFinder == nil {
			break
		}

		r, err := o.VariableFinder.FindVariableByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.OrganizationID, nil
	case influxdb.ScraperResourceType:
		if o.TargetFinder == nil {
			break
		}

		r, err := o.TargetFinder.GetTargetByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.OrgID, nil
	case influxdb.ChecksResourceType:
		if o.CheckFinder == nil {
			break
		}

		r, err := o.CheckFinder.FindCheckByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.GetOrgID(), nil
	case influxdb.NotificationEndpointResourceType:
		if o.NotificationEndpointFinder == nil {
			break
		}

		r, err := o.NotificationEndpointFinder.FindNotificationEndpointByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.GetOrgID(), nil
	case influxdb.NotificationRuleResourceType:
		if o.NotificationRuleFinder == nil {
			break
		}

		r, err := o.NotificationRuleFinder.FindNotificationRuleByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}

		return r.GetOrgID(), nil
	}

	return influxdb.InvalidID(), &influxdb.Error{
		Msg: fmt.Sprintf("unsupported resource type %s", rt),
	}
}

// FindResourceName is used to find the name of the resource associated with the provided type and id.
func (o *Resolver) FindResourceName(ctx context.Context, rt influxdb.ResourceType, id influxdb.ID) (string, error) {
	switch rt {
	case influxdb.AuthorizationsResourceType:
		// keeping this consistent with the original kv implementation
		return "", nil
	case influxdb.BucketsResourceType:
		if o.BucketFinder == nil {
			break
		}

		r, err := o.BucketFinder.FindBucketByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.Name, nil
	case influxdb.OrgsResourceType:
		if o.OrganizationFinder == nil {
			break
		}

		r, err := o.OrganizationFinder.FindOrganizationByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.Name, nil
	case influxdb.DashboardsResourceType:
		if o.DashboardFinder == nil {
			break
		}

		r, err := o.DashboardFinder.FindDashboardByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.Name, nil
	case influxdb.SourcesResourceType:
		if o.SourceFinder == nil {
			break
		}

		r, err := o.SourceFinder.FindSourceByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.Name, nil
	case influxdb.TasksResourceType:
		if o.TaskFinder == nil {
			break
		}

		r, err := o.TaskFinder.FindTaskByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.Name, nil
	case influxdb.TelegrafsResourceType:
		if o.TelegrafConfigFinder == nil {
			break
		}

		r, err := o.TelegrafConfigFinder.FindTelegrafConfigByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.Name, nil
	case influxdb.VariablesResourceType:
		if o.VariableFinder == nil {
			break
		}

		r, err := o.VariableFinder.FindVariableByID(ctx, id)
		if err != nil {
			return "", nil
		}

		return r.Name, nil
	case influxdb.ScraperResourceType:
		if o.TargetFinder == nil {
			break
		}

		r, err := o.TargetFinder.GetTargetByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.Name, nil
	case influxdb.ChecksResourceType:
		if o.CheckFinder == nil {
			break
		}

		r, err := o.CheckFinder.FindCheckByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.GetName(), nil
	case influxdb.NotificationEndpointResourceType:
		if o.NotificationEndpointFinder == nil {
			break
		}

		r, err := o.NotificationEndpointFinder.FindNotificationEndpointByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.GetName(), nil
	case influxdb.NotificationRuleResourceType:
		if o.NotificationRuleFinder == nil {
			break
		}

		r, err := o.NotificationRuleFinder.FindNotificationRuleByID(ctx, id)
		if err != nil {
			return "", err
		}

		return r.GetName(), nil
	}

	// default behaviour (in-line with original implementation) is to just return
	// an empty name
	return "", nil
}
