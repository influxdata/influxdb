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
		Default:   e.Default,
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
	e.Default = pb.Default

	return nil
}

// MarshalSource encodes a source to binary protobuf format.
func MarshalSource(s mrfusion.Source) ([]byte, error) {
	return proto.Marshal(&Source{
		ID:       int64(s.ID),
		Name:     s.Name,
		Type:     s.Type,
		Username: s.Username,
		Password: s.Password,
		URLs:     s.URL,
		Default:  s.Default,
	})
}

// UnmarshalSource decodes a source from binary protobuf data.
func UnmarshalSource(data []byte, s *mrfusion.Source) error {
	var pb Source
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	s.ID = int(pb.ID)
	s.Name = pb.Name
	s.Type = pb.Type
	s.Username = pb.Username
	s.Password = pb.Password
	s.URL = pb.URLs
	s.Default = pb.Default
	return nil
}

// MarshalServer encodes a server to binary protobuf format.
func MarshalServer(s mrfusion.Server) ([]byte, error) {
	return proto.Marshal(&Server{
		ID:       int64(s.ID),
		SrcID:    int64(s.SrcID),
		Name:     s.Name,
		Username: s.Username,
		Password: s.Password,
		URL:      s.URL,
	})
}

// UnmarshalServer decodes a server from binary protobuf data.
func UnmarshalServer(data []byte, s *mrfusion.Server) error {
	var pb Server
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	s.ID = int(pb.ID)
	s.SrcID = int(pb.SrcID)
	s.Name = pb.Name
	s.Username = pb.Username
	s.Password = pb.Password
	s.URL = pb.URL
	return nil
}

// MarshalLayout encodes a layout to binary protobuf format.
func MarshalLayout(l mrfusion.Layout) ([]byte, error) {
	cells := make([]*Cell, len(l.Cells))
	for i, c := range l.Cells {
		queries := make([]*Query, len(c.Queries))
		for j, q := range c.Queries {
			queries[j] = &Query{
				Command: q.Command,
				DB:      q.DB,
				RP:      q.RP,
			}
		}
		cells[i] = &Cell{
			X:       c.X,
			Y:       c.Y,
			W:       c.W,
			H:       c.H,
			Queries: queries,
		}
	}
	return proto.Marshal(&Layout{
		ID:          l.ID,
		Measurement: l.Measurement,
		Application: l.Application,
		Cells:       cells,
	})
}

// UnmarshalLayout decodes a layout from binary protobuf data.
func UnmarshalLayout(data []byte, l *mrfusion.Layout) error {
	var pb Layout
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	l.ID = pb.ID
	l.Measurement = pb.Measurement
	l.Application = pb.Application
	cells := make([]mrfusion.Cell, len(pb.Cells))
	for i, c := range pb.Cells {
		queries := make([]mrfusion.Query, len(c.Queries))
		for j, q := range c.Queries {
			queries[j] = mrfusion.Query{
				Command: q.Command,
				DB:      q.DB,
				RP:      q.RP,
			}
		}
		cells[i] = mrfusion.Cell{
			X:       c.X,
			Y:       c.Y,
			W:       c.W,
			H:       c.H,
			Queries: queries,
		}
	}
	l.Cells = cells
	return nil
}
