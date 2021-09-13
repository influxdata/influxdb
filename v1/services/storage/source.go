package storage

import (
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/v2"
)

func GetReadSource(any types.Any) (*ReadSource, error) {
	var source ReadSource
	if err := types.UnmarshalAny(&any, &source); err != nil {
		return nil, err
	}
	return &source, nil
}

func (r *ReadSource) GetOrgID() influxdb.ID {
	return influxdb.ID(r.OrgID)
}

func (r *ReadSource) GetBucketID() influxdb.ID {
	return influxdb.ID(r.BucketID)
}
