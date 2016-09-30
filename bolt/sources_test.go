package bolt_test

import (

	// "fmt"

	"reflect"
	"testing"

	"github.com/influxdata/mrfusion"
)

// Ensure an SourceStore can store, retrieve, update, and delete explorations.
func TestSourceStore(t *testing.T) {
	c, err := NewTestClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := c.SourcesStore

	srcs := []mrfusion.Source{
		mrfusion.Source{
			Name:     "Of Truth",
			Type:     "influx",
			Username: "marty",
			Password: "I❤️  jennifer parker",
			URL:      []string{"toyota-hilux.lyon-estates.local", "lake.hilldale.local"},
			Default:  true,
		},
		mrfusion.Source{
			Name:     "HipToBeSquare",
			Type:     "influx",
			Username: "calvinklein",
			Password: "chuck b3rry",
			URL:      []string{"toyota-hilux.lyon-estates.local", "lake.hilldale.local"},
			Default:  true,
		},
	}

	// Add new srcs.
	for i, src := range srcs {
		if srcs[i], err = s.Add(nil, src); err != nil {
			t.Fatal(err)
		}
		// Confirm first src in the store is the same as the original.
		if actual, err := s.Get(nil, srcs[i].ID); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(actual, srcs[i]) {
			t.Fatal("source loaded is different then source saved; actual: %v, expected %v", actual, srcs[i])
		}
	}

	// Update source.
	srcs[0].Username = "calvinklein"
	srcs[1].Name = "Enchantment Under the Sea Dance"
	if err := s.Update(nil, srcs[0]); err != nil {
		t.Fatal(err)
	} else if err := s.Update(nil, srcs[1]); err != nil {
		t.Fatal(err)
	}

	// Confirm sources have updated.
	if src, err := s.Get(nil, srcs[0].ID); err != nil {
		t.Fatal(err)
	} else if src.Username != "calvinklein" {
		t.Fatalf("source 0 update error: got %v, expected %v", src.Username, "calvinklein")
	}
	if src, err := s.Get(nil, srcs[1].ID); err != nil {
		t.Fatal(err)
	} else if src.Name != "Enchantment Under the Sea Dance" {
		t.Fatalf("source 1 update error: got %v, expected %v", src.Name, "Enchantment Under the Sea Dance")
	}

	// Delete an source.
	if err := s.Delete(nil, srcs[0]); err != nil {
		t.Fatal(err)
	}

	// Confirm source has been deleted.
	if _, err := s.Get(nil, srcs[0].ID); err != mrfusion.ErrSourceNotFound {
		t.Fatalf("source delete error: got %v, expected %v", err, mrfusion.ErrSourceNotFound)
	}

	if bsrcs, err := s.All(nil); err != nil {
		t.Fatal(err)
	} else if len(bsrcs) != 1 {
		t.Fatalf("After delete All returned incorrect number of srcs; got %d, expected %d", len(bsrcs), 1)
	} else if !reflect.DeepEqual(bsrcs[0], srcs[1]) {
		t.Fatalf("After delete All returned incorrect source; got %v, expected %v", bsrcs[0], srcs[1])
	}
}
