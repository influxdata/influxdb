package storage

import (
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

// this is easier than fooling around with .proto files.

type readSource struct {
	BucketID       uint64 `protobuf:"varint,1,opt,name=bucket_id,proto3"`
	OrganizationID uint64 `protobuf:"varint,2,opt,name=organization_id,proto3"`
}

func (r *readSource) XXX_MessageName() string { return "readSource" }
func (r *readSource) Reset()                  { *r = readSource{} }
func (r *readSource) String() string          { return "readSource{}" }
func (r *readSource) ProtoMessage()           {}

func getReadSource(any types.Any) (readSource, error) {
	var source readSource
	if err := types.UnmarshalAny(&any, &source); err != nil {
		return source, err
	}
	return source, nil
}

func (r *readSource) GetOrgID() platform.ID {
	return platform.ID(r.OrganizationID)
}

func (r *readSource) GetBucketID() platform.ID {
	return platform.ID(r.BucketID)
}
