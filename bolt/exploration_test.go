package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/chronograf"
)

// Ensure an ExplorationStore can store, retrieve, update, and delete explorations.
func TestExplorationStore_CRUD(t *testing.T) {
	c, err := NewTestClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := c.ExplorationStore

	explorations := []*chronograf.Exploration{
		&chronograf.Exploration{
			Name:   "Ferdinand Magellan",
			UserID: 2,
			Data:   "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		},
		&chronograf.Exploration{
			Name:   "Marco Polo",
			UserID: 3,
			Data:   "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		},
		&chronograf.Exploration{
			Name:   "Leif Ericson",
			UserID: 3,
			Data:   "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		},
	}

	ctx := context.Background()
	// Add new explorations.
	for i := range explorations {
		if _, err := s.Add(ctx, explorations[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Confirm first exploration in the store is the same as the original.
	if e, err := s.Get(ctx, explorations[0].ID); err != nil {
		t.Fatal(err)
	} else if e.ID != explorations[0].ID {
		t.Fatalf("exploration ID error: got %v, expected %v", e.ID, explorations[1].ID)
	} else if e.Name != explorations[0].Name {
		t.Fatalf("exploration Name error: got %v, expected %v", e.Name, explorations[1].Name)
	} else if e.UserID != explorations[0].UserID {
		t.Fatalf("exploration UserID error: got %v, expected %v", e.UserID, explorations[1].UserID)
	} else if e.Data != explorations[0].Data {
		t.Fatalf("exploration Data error: got %v, expected %v", e.Data, explorations[1].Data)
	}

	// Update explorations.
	explorations[1].Name = "Francis Drake"
	explorations[2].UserID = 4
	if err := s.Update(ctx, explorations[1]); err != nil {
		t.Fatal(err)
	} else if err := s.Update(ctx, explorations[2]); err != nil {
		t.Fatal(err)
	}

	// Confirm explorations are updated.
	if e, err := s.Get(ctx, explorations[1].ID); err != nil {
		t.Fatal(err)
	} else if e.Name != "Francis Drake" {
		t.Fatalf("exploration 1 update error: got %v, expected %v", e.Name, "Francis Drake")
	}
	if e, err := s.Get(ctx, explorations[2].ID); err != nil {
		t.Fatal(err)
	} else if e.UserID != 4 {
		t.Fatalf("exploration 2 update error: got %v, expected %v", e.UserID, 4)
	}

	// Delete an exploration.
	if err := s.Delete(ctx, explorations[2]); err != nil {
		t.Fatal(err)
	}

	// Confirm exploration has been deleted.
	if e, err := s.Get(ctx, explorations[2].ID); err != chronograf.ErrExplorationNotFound {
		t.Fatalf("exploration delete error: got %v, expected %v", e, chronograf.ErrExplorationNotFound)
	}
}

// Ensure Explorations can be queried by UserID.
func TestExplorationStore_Query(t *testing.T) {
	c, err := NewTestClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := c.ExplorationStore

	explorations := []*chronograf.Exploration{
		&chronograf.Exploration{
			Name:   "Ferdinand Magellan",
			UserID: 2,
			Data:   "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		},
		&chronograf.Exploration{
			Name:   "Marco Polo",
			UserID: 3,
			Data:   "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		},
		&chronograf.Exploration{
			Name:   "Leif Ericson",
			UserID: 3,
			Data:   "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
		},
	}

	ctx := context.Background()
	// Add new explorations.
	for i := range explorations {
		if _, err := s.Add(ctx, explorations[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Query for explorations.
	if e, err := s.Query(ctx, 3); err != nil {
		t.Fatal(err)
	} else if len(e) != 2 {
		t.Fatalf("exploration query length error: got %v, expected %v", len(explorations), len(e))
	} else if e[0].Name != explorations[1].Name {
		t.Fatalf("exploration query  error: got %v, expected %v", explorations[0].Name, "Marco Polo")
	} else if e[1].Name != explorations[2].Name {
		t.Fatalf("exploration query  error: got %v, expected %v", explorations[1].Name, "Leif Ericson")
	}

}
