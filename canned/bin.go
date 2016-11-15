package canned

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/chronograf"
)

//go:generate go-bindata -o bin_gen.go -ignore README|apps|.sh|go -pkg canned .

type BinLayoutStore struct {
	Logger chronograf.Logger
}

// All returns the set of all layouts
func (s *BinLayoutStore) All(ctx context.Context) ([]chronograf.Layout, error) {
	names := AssetNames()
	layouts := make([]chronograf.Layout, len(names))
	for i, name := range names {
		octets, err := Asset(name)
		if err != nil {
			s.Logger.
				WithField("component", "apps").
				WithField("name", name).
				Error("Invalid Layout: ", err)
			return nil, chronograf.ErrLayoutInvalid
		}

		var layout chronograf.Layout
		if err = json.Unmarshal(octets, &layout); err != nil {
			s.Logger.
				WithField("component", "apps").
				WithField("name", name).
				Error("Unable to read layout:", err)
			return nil, chronograf.ErrLayoutInvalid
		}
		layouts[i] = layout
	}

	return layouts, nil
}

// Add is not support by BinLayoutStore
func (s *BinLayoutStore) Add(ctx context.Context, layout chronograf.Layout) (chronograf.Layout, error) {
	return chronograf.Layout{}, fmt.Errorf("Add to BinLayoutStore not supported")
}

// Delete is not support by BinLayoutStore
func (s *BinLayoutStore) Delete(ctx context.Context, layout chronograf.Layout) error {
	return fmt.Errorf("Delete to BinLayoutStore not supported")
}

// Get retrieves Layout if `ID` exists.
func (s *BinLayoutStore) Get(ctx context.Context, ID string) (chronograf.Layout, error) {
	layouts, err := s.All(ctx)
	if err != nil {
		s.Logger.
			WithField("component", "apps").
			WithField("name", ID).
			Error("Invalid Layout: ", err)
		return chronograf.Layout{}, chronograf.ErrLayoutInvalid
	}

	for _, layout := range layouts {
		if layout.ID == ID {
			return layout, nil
		}
	}

	s.Logger.
		WithField("component", "apps").
		WithField("name", ID).
		Error("Layout not found")
	return chronograf.Layout{}, chronograf.ErrLayoutNotFound
}

// Update not supported
func (s *BinLayoutStore) Update(ctx context.Context, layout chronograf.Layout) error {
	return fmt.Errorf("Update to BinLayoutStore not supported")
}
