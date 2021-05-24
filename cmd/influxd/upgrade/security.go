package upgrade

// Security upgrade implementation.
// Creates tokens representing v1 users.

import (
	"context"
	"errors"
	"fmt"
	"sort"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// upgradeUsers creates tokens representing v1 users.
func upgradeUsers(
	ctx context.Context,
	v1 *influxDBv1,
	v2 *influxDBv2,
	targetOptions *optionsV2,
	dbBuckets map[string][]platform2.ID,
	log *zap.Logger,
) (int, error) {
	// check if there any 1.x users at all
	v1meta := v1.meta
	if len(v1meta.Users()) == 0 {
		log.Info("There are no users in 1.x, nothing to upgrade")
		return 0, nil
	}

	// get helper instance
	helper := newSecurityUpgradeHelper(log)

	// check if target buckets exists in 2.x
	proceed := helper.checkDbBuckets(v1meta, dbBuckets)
	if !proceed {
		return 0, errors.New("upgrade: there were errors/warnings, please fix them and run the command again")
	}

	// upgrade users
	log.Info("Upgrading 1.x users")
	numUpgraded := 0
	for _, row := range helper.sortUserInfo(v1meta.Users()) {
		username := row.Name
		if row.Admin {
			log.Warn("User is admin and will not be upgraded", zap.String("username", username))
		} else if len(row.Privileges) == 0 {
			log.Warn("User has no privileges and will not be upgraded", zap.String("username", username))
		} else {
			var dbList []string
			for database := range row.Privileges {
				dbList = append(dbList, database)
			}
			var permissions []platform.Permission
			for _, database := range dbList {
				permission := row.Privileges[database]
				for _, id := range dbBuckets[database] {
					switch permission {
					case influxql.ReadPrivilege:
						p, err := platform.NewPermissionAtID(id, platform.ReadAction, platform.BucketsResourceType, targetOptions.orgID)
						if err != nil {
							return numUpgraded, err
						}
						permissions = append(permissions, *p)
					case influxql.WritePrivilege:
						p, err := platform.NewPermissionAtID(id, platform.WriteAction, platform.BucketsResourceType, targetOptions.orgID)
						if err != nil {
							return numUpgraded, err
						}
						permissions = append(permissions, *p)
					case influxql.AllPrivileges:
						p, err := platform.NewPermissionAtID(id, platform.ReadAction, platform.BucketsResourceType, targetOptions.orgID)
						if err != nil {
							return numUpgraded, err
						}
						permissions = append(permissions, *p)
						p, err = platform.NewPermissionAtID(id, platform.WriteAction, platform.BucketsResourceType, targetOptions.orgID)
						if err != nil {
							return numUpgraded, err
						}
						permissions = append(permissions, *p)
					}
				}
			}
			if len(permissions) > 0 {
				auth := &platform.Authorization{
					Description: username + "'s Legacy Token",
					Permissions: permissions,
					Token:       username,
					OrgID:       targetOptions.orgID,
					UserID:      targetOptions.userID,
				}
				err := v2.authSvc.CreateAuthorization(ctx, auth)
				if err != nil {
					log.Error("Failed to create authorization", zap.String("user", username), zap.Error(err))
					continue
				}
				err = v2.authSvc.SetPasswordHash(ctx, auth.ID, row.Hash)
				if err != nil {
					log.Error("Failed to set user's password", zap.String("user", username), zap.Error(err))
					continue
				}
				log.Debug("User upgraded", zap.String("username", username))
				numUpgraded++
			} else {
				log.Warn("User has no privileges and will not be upgraded", zap.String("username", username))
			}
		}
	}

	log.Info("User upgrade complete", zap.Int("upgraded_count", numUpgraded))
	return numUpgraded, nil
}

// securityUpgradeHelper is a helper used by `upgrade` command.
type securityUpgradeHelper struct {
	log *zap.Logger
}

// newSecurityUpgradeHelper returns new security script helper instance for `upgrade` command.
func newSecurityUpgradeHelper(log *zap.Logger) *securityUpgradeHelper {
	helper := &securityUpgradeHelper{
		log: log,
	}

	return helper
}
func (h *securityUpgradeHelper) checkDbBuckets(meta *meta.Client, databases map[string][]platform2.ID) bool {
	ok := true
	for _, row := range meta.Users() {
		for database := range row.Privileges {
			if database == "_internal" {
				continue
			}
			ids := databases[database]
			if len(ids) == 0 {
				h.log.Warn(fmt.Sprintf("No buckets for database [%s] exist in 2.x.", database))
				ok = false
			}
		}
	}

	return ok
}

func (h *securityUpgradeHelper) sortUserInfo(info []meta.UserInfo) []meta.UserInfo {
	sort.Slice(info, func(i, j int) bool {
		return info[i].Name < info[j].Name
	})
	return info
}
