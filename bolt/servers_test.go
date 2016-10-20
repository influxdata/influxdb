package bolt_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
)

// Ensure an ServerStore can store, retrieve, update, and delete servers.
func TestServerStore(t *testing.T) {
	c, err := NewTestClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := c.ServersStore

	srcs := []chronograf.Server{
		chronograf.Server{
			Name:     "Of Truth",
			SrcID:    10,
			Username: "marty",
			Password: "I❤️  jennifer parker",
			URL:      "toyota-hilux.lyon-estates.local",
		},
		chronograf.Server{
			Name:     "HipToBeSquare",
			SrcID:    12,
			Username: "calvinklein",
			Password: "chuck b3rry",
			URL:      "toyota-hilux.lyon-estates.local",
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
			t.Fatal("server loaded is different then server saved; actual: %v, expected %v", actual, srcs[i])
		}
	}

	// Update server.
	srcs[0].Username = "calvinklein"
	srcs[1].Name = "Enchantment Under the Sea Dance"
	if err := s.Update(nil, srcs[0]); err != nil {
		t.Fatal(err)
	} else if err := s.Update(nil, srcs[1]); err != nil {
		t.Fatal(err)
	}

	// Confirm servers have updated.
	if src, err := s.Get(nil, srcs[0].ID); err != nil {
		t.Fatal(err)
	} else if src.Username != "calvinklein" {
		t.Fatalf("server 0 update error: got %v, expected %v", src.Username, "calvinklein")
	}
	if src, err := s.Get(nil, srcs[1].ID); err != nil {
		t.Fatal(err)
	} else if src.Name != "Enchantment Under the Sea Dance" {
		t.Fatalf("server 1 update error: got %v, expected %v", src.Name, "Enchantment Under the Sea Dance")
	}

	// Delete an server.
	if err := s.Delete(nil, srcs[0]); err != nil {
		t.Fatal(err)
	}

	// Confirm server has been deleted.
	if _, err := s.Get(nil, srcs[0].ID); err != chronograf.ErrServerNotFound {
		t.Fatalf("server delete error: got %v, expected %v", err, chronograf.ErrServerNotFound)
	}

	if bsrcs, err := s.All(nil); err != nil {
		t.Fatal(err)
	} else if len(bsrcs) != 1 {
		t.Fatalf("After delete All returned incorrect number of srcs; got %d, expected %d", len(bsrcs), 1)
	} else if !reflect.DeepEqual(bsrcs[0], srcs[1]) {
		t.Fatalf("After delete All returned incorrect server; got %v, expected %v", bsrcs[0], srcs[1])
	}
}
