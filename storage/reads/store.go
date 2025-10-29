package reads

import (
	"context"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"google.golang.org/protobuf/proto"
)

type ResultSet interface {
	// Next advances the ResultSet to the next cursor. It returns false
	// when there are no more cursors.
	Next() bool

	// Cursor returns the most recent cursor after a call to Next.
	Cursor() (cursors.Cursor, error)

	// Tags returns the tags for the most recent cursor after a call to Next.
	Tags() models.Tags

	// Close releases any resources allocated by the ResultSet.
	Close()

	// Err returns the first error encountered by the ResultSet.
	Err() error

	Stats() cursors.CursorStats
}

type GroupResultSet interface {
	// Next advances the GroupResultSet and returns the next GroupCursor. It
	// returns nil if there are no more groups.
	Next() GroupCursor

	// Close releases any resources allocated by the GroupResultSet.
	Close()

	// Err returns the first error encountered by the GroupResultSet.
	Err() error
}

type GroupCursor interface {
	// Next advances to the next cursor. Next will return false when there are no
	// more cursors in the current group.
	Next() bool

	// Cursor returns the most recent cursor after a call to Next.
	Cursor() (cursors.Cursor, error)

	// Tags returns the tags for the most recent cursor after a call to Next.
	Tags() models.Tags

	// Keys returns the union of all tag key names for all series produced by
	// this GroupCursor.
	Keys() [][]byte

	// PartitionKeyVals returns the values of all tags identified by the
	// keys specified in ReadRequest#GroupKeys. The tag values values will
	// appear in the same order as the GroupKeys.
	//
	// When the datatypes.GroupNone strategy is specified, PartitionKeyVals will
	// be nil.
	PartitionKeyVals() [][]byte

	// Close releases any resources allocated by the GroupCursor.
	Close()

	// Err returns the first error encountered by the GroupCursor.
	Err() error

	Stats() cursors.CursorStats

	Aggregate() *datatypes.Aggregate
}

type Store interface {
	ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (ResultSet, error)
	ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (GroupResultSet, error)
	// WindowAggregate will invoke a ReadWindowAggregateRequest against the Store.
	WindowAggregate(ctx context.Context, req *datatypes.ReadWindowAggregateRequest) (ResultSet, error)

	TagKeys(ctx context.Context, req *datatypes.TagKeysRequest) (cursors.StringIterator, error)
	TagValues(ctx context.Context, req *datatypes.TagValuesRequest) (cursors.StringIterator, error)

	ReadSeriesCardinality(ctx context.Context, req *datatypes.ReadSeriesCardinalityRequest) (cursors.Int64Iterator, error)
	SupportReadSeriesCardinality(ctx context.Context) bool

	GetSource(orgID, bucketID uint64) proto.Message
}
