package upgrade

import (
	"context"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/errors"
	"go.uber.org/zap"
)

// upgradeDatabases creates databases, buckets, retention policies and shard info according to 1.x meta and copies data
func upgradeDatabases(ctx context.Context, v1 *influxDBv1, v2 *influxDBv2, orgID influxdb.ID, log *zap.Logger) (map[string][]string, error) {
	return nil, errors.New("not implemented")
}
