package feature

import (
	"context"
	"errors"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
)

var ErrMissingTargetInfo = errors.New("unable to determine any user or org IDs from authorizer on context")

// Target against which to match a feature flag rule.
type Target struct {
	// UserID to Target.
	UserID influxdb.ID
	// OrgIDs to Target.
	OrgIDs []influxdb.ID
}

// MakeTarget returns a populated feature flag Target for the given environment,
// including user and org information from the provided context, if available.
//
// If the authorizer on the context provides a user ID, it is used to fetch associated org IDs.
// If a user ID is not provided, an org ID is taken directly off the authorizer if possible.
// If no user or org information can be determined, a sentinel error is returned.
func MakeTarget(ctx context.Context, urms influxdb.UserResourceMappingService) (Target, error) {
	auth, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return Target{}, ErrMissingTargetInfo
	}
	userID := auth.GetUserID()

	var orgIDs []influxdb.ID
	if userID.Valid() {
		orgIDs, err = fromURMs(ctx, userID, urms)
		if err != nil {
			return Target{}, err
		}
	} else if a, ok := auth.(*influxdb.Authorization); ok {
		orgIDs = []influxdb.ID{a.OrgID}
	} else {
		return Target{}, ErrMissingTargetInfo
	}

	return Target{
		UserID: userID,
		OrgIDs: orgIDs,
	}, nil
}

func fromURMs(ctx context.Context, userID influxdb.ID, urms influxdb.UserResourceMappingService) ([]influxdb.ID, error) {
	m, _, err := urms.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
		UserID:       userID,
		ResourceType: influxdb.OrgsResourceType,
	})
	if err != nil {
		return nil, fmt.Errorf("finding organization mappings for user %s: %v", userID, err)
	}

	orgIDs := make([]influxdb.ID, 0, len(m))
	for _, o := range m {
		orgIDs = append(orgIDs, o.ResourceID)
	}

	return orgIDs, nil
}
