package influxdb

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

type Instance struct {
	ID platform.ID `json:"id" db:"id"`
}

type InstanceService interface {
	CreateInstance(ctx context.Context) (*Instance, error)

	GetInstance(ctx context.Context) (*Instance, error)
}
