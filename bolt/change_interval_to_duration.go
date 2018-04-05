package bolt

import (
	"log"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
)

// changeIntervalToDuration
// Before, we supported queries that included `GROUP BY :interval:`
// After, we only support queries with `GROUP BY time(:interval:)`
// thereby allowing non_negative_derivative(_____, :interval)
var changeIntervalToDuration = Migration{
	ID:   "59b0cda4fc7909ff84ee5c4f9cb4b655b6a26620",
	Up:   up,
	Down: down,
}

func updateDashboard(board *Dashboard) {
	for _, cell := range board.Cells {
		for _, query := range cell.Queries {
			query.Command = strings.Replace(query.Command, ":interval:", "time(:interval:)", -1)
		}
	}
}

var up = func(db *bolt.DB) error {
	// For each dashboard
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(dashboardBucket)
		err := bucket.ForEach(func(id, data []byte) error {
			board := &Dashboard{}

			err := proto.Unmarshal(data, board)
			if err != nil {
				log.Fatal("unmarshaling error: ", err)
			}

			// Migrate the dashboard
			updateDashboard(board)

			data, err = proto.Marshal(board)
			if err != nil {
				log.Fatal("marshaling error: ", err)
			}

			err = bucket.Put(id, data)
			if err != nil {
				log.Fatal("error updating dashboard: ", err)
			}

			return nil
		})

		if err != nil {
			log.Fatal("error updating dashboards: ", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

var down = func(db *bolt.DB) error {
	return nil
}

/*
	Import protobuf types and bucket names that are pertinent to this migration.
	This isolates the migration from the codebase, and prevents a future change
	to a type definition from invalidating the migration functions.
*/
var dashboardBucket = []byte("Dashoard")

type Dashboard struct {
	ID           int64            `protobuf:"varint,1,opt,name=ID,proto3" json:"ID,omitempty"`
	Name         []byte           `protobuf:"bytes,2,opt,name=Name,proto3" json:"Name,omitempty"`
	Cells        []*DashboardCell `protobuf:"bytes,3,rep,name=cells" json:"cells,omitempty"`
	Templates    []byte           `protobuf:"bytes,4,rep,name=templates" json:"templates,omitempty"`
	Organization []byte           `protobuf:"bytes,5,opt,name=Organization,proto3" json:"Organization,omitempty"`
}

func (*Dashboard) ProtoMessage()    {}
func (m *Dashboard) Reset()         { *m = Dashboard{} }
func (m *Dashboard) String() string { return proto.CompactTextString(m) }

type DashboardCell struct {
	X            int32    `protobuf:"varint,1,opt,name=x,proto3" json:"x,omitempty"`
	Y            int32    `protobuf:"varint,2,opt,name=y,proto3" json:"y,omitempty"`
	W            int32    `protobuf:"varint,3,opt,name=w,proto3" json:"w,omitempty"`
	H            int32    `protobuf:"varint,4,opt,name=h,proto3" json:"h,omitempty"`
	Queries      []*Query `protobuf:"bytes,5,rep,name=queries" json:"queries,omitempty"`
	Name         []byte   `protobuf:"bytes,6,opt,name=name,proto3" json:"name,omitempty"`
	Type         []byte   `protobuf:"bytes,7,opt,name=type,proto3" json:"type,omitempty"`
	ID           []byte   `protobuf:"bytes,8,opt,name=ID,proto3" json:"ID,omitempty"`
	Axes         []byte   `protobuf:"bytes,9,rep,name=axes" json:"axes,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value"`
	Colors       []byte   `protobuf:"bytes,10,rep,name=colors" json:"colors,omitempty"`
	Legend       []byte   `protobuf:"bytes,11,opt,name=legend" json:"legend,omitempty"`
	TableOptions []byte   `protobuf:"bytes,12,opt,name=tableOptions" json:"tableOptions,omitempty"`
}

func (m *DashboardCell) Reset()         { *m = DashboardCell{} }
func (m *DashboardCell) String() string { return proto.CompactTextString(m) }
func (*DashboardCell) ProtoMessage()    {}

type Query struct {
	Command  string `protobuf:"bytes,1,opt,name=Command,proto3" json:"Command,omitempty"`
	DB       []byte `protobuf:"bytes,2,opt,name=DB,proto3" json:"DB,omitempty"`
	RP       []byte `protobuf:"bytes,3,opt,name=RP,proto3" json:"RP,omitempty"`
	GroupBys []byte `protobuf:"bytes,4,rep,name=GroupBys" json:"GroupBys,omitempty"`
	Wheres   []byte `protobuf:"bytes,5,rep,name=Wheres" json:"Wheres,omitempty"`
	Label    []byte `protobuf:"bytes,6,opt,name=Label,proto3" json:"Label,omitempty"`
	Range    []byte `protobuf:"bytes,7,opt,name=Range" json:"Range,omitempty"`
	Source   []byte `protobuf:"bytes,8,opt,name=Source,proto3" json:"Source,omitempty"`
	Shifts   []byte `protobuf:"bytes,9,rep,name=Shifts" json:"Shifts,omitempty"`
}

func (m *Query) Reset()         { *m = Query{} }
func (m *Query) String() string { return proto.CompactTextString(m) }
func (*Query) ProtoMessage()    {}
