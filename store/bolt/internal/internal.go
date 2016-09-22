package internal

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/mrfusion"
)

//go:generate protoc --gogo_out=. internal.proto

// MarshalExploration encodes an exploration to binary protobuf format.
func MarshalExploration(e *mrfusion.Exploration) ([]byte, error) {
	return proto.Marshal(&Exploration{
		ID:        int64(e.ID),
		Name:      e.Name,
		UserID:    int64(e.UserID),
		Data:      e.Data,
		CreatedAt: e.CreatedAt.UnixNano(),
		UpdatedAt: e.UpdatedAt.UnixNano(),
	})
}

// UnmarshalExploration decodes an exploration from binary protobuf data.
func UnmarshalExploration(data []byte, e *mrfusion.Exploration) error {
	var pb Exploration
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	e.ID = mrfusion.ExplorationID(pb.ID)
	e.Name = pb.Name
	e.UserID = mrfusion.UserID(pb.UserID)
	e.Data = pb.Data
	e.CreatedAt = time.Unix(0, pb.CreatedAt).UTC()
	e.UpdatedAt = time.Unix(0, pb.UpdatedAt).UTC()

	return nil
}
