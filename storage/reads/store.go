package reads

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/platform/models"
	fstorage "github.com/influxdata/platform/query/functions/inputs/storage"
	"github.com/influxdata/platform/storage/reads/datatypes"
	"github.com/influxdata/platform/tsdb/cursors"
)

type ResultSet interface {
	Close()
	Next() bool
	Cursor() cursors.Cursor
	Tags() models.Tags
}

type GroupResultSet interface {
	Next() GroupCursor
	Close()
}

type GroupCursor interface {
	Tags() models.Tags
	Keys() [][]byte
	PartitionKeyVals() [][]byte
	Next() bool
	Cursor() cursors.Cursor
	Close()
}

type Store interface {
	Read(ctx context.Context, req *datatypes.ReadRequest) (ResultSet, error)
	GroupRead(ctx context.Context, req *datatypes.ReadRequest) (GroupResultSet, error)
	GetSource(rs fstorage.ReadSpec) (proto.Message, error)
}
