package bolt_test

import (
	"testing"

	"github.com/influxdata/mrfusion"
)

// Ensure Exploration can be added and retrieved.
func TestExplorationStore_Add(t *testing.T) {
	c := MustOpenClient()
	defer c.Close()
	s := c.Connect().ExplorationStore()

	data := "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}"

	exploration := mrfusion.Exploration{
		Name:   "Ferdinand Magellan",
		UserID: 2,
		Data:   data,
	}

	// Add new exploration.
	if err := s.Add(nil, exploration); err != nil {
		t.Fatal(err)
	} else if exploration.ID != 0 {
		t.Fatalf("exploration ID error: got %v, expected %v", exploration.ID, 1)
	}

	// Confirm exploration in the store is the same as the original.
	e, err := s.Get(nil, 1)
	if err != nil {
		t.Fatal(err)
	} else if e.Name != exploration.Name {
		t.Fatalf("exploration Name error: got %v, expected %v", e.Name, exploration.Name)
	} else if e.UserID != exploration.UserID {
		t.Fatalf("exploration UserID error: got %v, expected %v", e.UserID, exploration.UserID)
	} else if e.Data != exploration.Data {
		t.Fatalf("exploration Data error: got %v, expected %v", e.Data, exploration.Data)
	}
}

// Ensure Explorations can be queried by UserID.
func TestExplorationStore_Query(t *testing.T) {
	c := MustOpenClient()
	defer c.Close()
	s := c.Connect().ExplorationStore()

	explorations := make(map[int]mrfusion.Exploration)
	explorations[0] = mrfusion.Exploration{
		Name:   "Ferdinand Magellan",
		UserID: 2,
		Data:   "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
	}
	explorations[1] = mrfusion.Exploration{
		Name:   "Marco Polo",
		UserID: 3,
		Data:   "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
	}
	explorations[2] = mrfusion.Exploration{
		Name:   "Leif Ericson",
		UserID: 3,
		Data:   "{\"panels\":{\"123\":{\"id\":\"123\",\"queryIds\":[\"456\"]}},\"queryConfigs\":{\"456\":{\"id\":\"456\",\"database\":null,\"measurement\":null,\"retentionPolicy\":null,\"fields\":[],\"tags\":{},\"groupBy\":{\"time\":null,\"tags\":[]},\"areTagsAccepted\":true,\"rawText\":null}}}",
	}

	for i := range explorations {
		if err := s.Add(nil, explorations[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Query for explorations.
	if explorations, err := s.Query(nil, 3); err != nil {
		t.Fatal(err)
	} else if len(explorations) != 2 {
		t.Fatalf("exploration query length error: got %v, expected %v", len(explorations), 2)
	} else if explorations[0].Name != "Marco Polo" {
		t.Fatalf("exploration query  error: got %v, expected %v", explorations[0].Name, "Marco Polo")
	} else if explorations[1].Name != "Leif Ericson" {
		t.Fatalf("exploration query  error: got %v, expected %v", explorations[1].Name, "Leif Ericson")
	}

}

// Ensure an exploration can be deleted.
func TestExplorationStore_Delete(t *testing.T) {
	// TODO: Make sure deleting an exploration works.
	t.Skip()
}

// Ensure explorations can be updated.
func TestExplorationStore_Update(t *testing.T) {
	c := MustOpenClient()
	defer c.Close()
	s := c.Connect().ExplorationStore()

	if err := s.Add(nil, mrfusion.Exploration{Name: "Ferdinand Magellan"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(nil, mrfusion.Exploration{UserID: 3}); err != nil {
		t.Fatal(err)
	}

	// Update explorations.
	if err := s.Update(nil, mrfusion.Exploration{ID: 1, Name: "Francis Drake"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(nil, mrfusion.Exploration{ID: 2, UserID: 4}); err != nil {
		t.Fatal(err)
	}

	// Confirm first Exploration update updated Name.
	if e, err := s.Get(nil, 1); err != nil {
		t.Fatal(err)
	} else if e.Name != "Francis Drake" {
		t.Fatalf("exploration 1 update error: got %v, expected %v", e.Name, "Francis Drake")
	}

	// Confirm second Exploration has updated UserID.
	if e, err := s.Get(nil, 2); err != nil {
		t.Fatal(err)
	} else if e.UserID != 4 {
		t.Fatalf("exploration 2 update error: got: %v, expected: %v", e.UserID, 4)
	}
}
