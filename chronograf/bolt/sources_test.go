package bolt_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/influxdata/platform/chronograf"
	"github.com/influxdata/platform/chronograf/bolt"
)

// Ensure an SourceStore can store, retrieve, update, and delete sources.
func TestSourceStore(t *testing.T) {
	c, err := NewTestClient()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	s := c.SourcesStore

	srcs := []chronograf.Source{
		chronograf.Source{
			Name:         "Of Truth",
			Type:         "influx",
			Username:     "marty",
			Password:     "I❤️  jennifer parker",
			URL:          "toyota-hilux.lyon-estates.local",
			Default:      true,
			Organization: "1337",
			DefaultRP:    "pineapple",
		},
		chronograf.Source{
			Name:         "HipToBeSquare",
			Type:         "influx",
			Username:     "calvinklein",
			Password:     "chuck b3rry",
			URL:          "toyota-hilux.lyon-estates.local",
			Default:      true,
			Organization: "1337",
		},
		chronograf.Source{
			Name:               "HipToBeSquare",
			Type:               "influx",
			Username:           "calvinklein",
			Password:           "chuck b3rry",
			URL:                "https://toyota-hilux.lyon-estates.local",
			InsecureSkipVerify: true,
			Default:            false,
			Organization:       "1337",
		},
	}

	ctx := context.Background()
	// Add new srcs.
	for i, src := range srcs {
		if srcs[i], err = s.Add(ctx, src); err != nil {
			t.Fatal(err)
		}
		// Confirm first src in the store is the same as the original.
		if actual, err := s.Get(ctx, srcs[i].ID); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(actual, srcs[i]) {
			t.Fatalf("source loaded is different then source saved; actual: %v, expected %v", actual, srcs[i])
		}
	}

	// Update source.
	srcs[0].Username = "calvinklein"
	srcs[1].Name = "Enchantment Under the Sea Dance"
	srcs[2].DefaultRP = "cubeapple"
	mustUpdateSource(t, s, srcs[0])
	mustUpdateSource(t, s, srcs[1])
	mustUpdateSource(t, s, srcs[2])

	// Confirm sources have updated.
	if src, err := s.Get(ctx, srcs[0].ID); err != nil {
		t.Fatal(err)
	} else if src.Username != "calvinklein" {
		t.Fatalf("source 0 update error: got %v, expected %v", src.Username, "calvinklein")
	}
	if src, err := s.Get(ctx, srcs[1].ID); err != nil {
		t.Fatal(err)
	} else if src.Name != "Enchantment Under the Sea Dance" {
		t.Fatalf("source 1 update error: got %v, expected %v", src.Name, "Enchantment Under the Sea Dance")
	}
	if src, err := s.Get(ctx, srcs[2].ID); err != nil {
		t.Fatal(err)
	} else if src.DefaultRP != "cubeapple" {
		t.Fatalf("source 2 update error: got %v, expected %v", src.DefaultRP, "cubeapple")
	}

	// Attempt to make two default sources
	srcs[0].Default = true
	srcs[1].Default = true
	mustUpdateSource(t, s, srcs[0])
	mustUpdateSource(t, s, srcs[1])

	if actual, err := s.Get(ctx, srcs[0].ID); err != nil {
		t.Fatal(err)
	} else if actual.Default == true {
		t.Fatal("Able to set two default sources when only one should be permitted")
	}

	// Attempt to add a new default source
	srcs = append(srcs, chronograf.Source{
		Name:         "Biff Tannen",
		Type:         "influx",
		Username:     "HELLO",
		Password:     "MCFLY",
		URL:          "anybody.in.there.local",
		Default:      true,
		Organization: "1892",
	})

	srcs[3] = mustAddSource(t, s, srcs[3])
	if srcs, err := s.All(ctx); err != nil {
		t.Fatal(err)
	} else {
		defaults := 0
		for _, src := range srcs {
			if src.Default {
				defaults++
			}
		}

		if defaults != 1 {
			t.Fatal("Able to add more than one default source")
		}
	}

	// Delete an source.
	if err := s.Delete(ctx, srcs[0]); err != nil {
		t.Fatal(err)
	}

	// Confirm source has been deleted.
	if _, err := s.Get(ctx, srcs[0].ID); err != chronograf.ErrSourceNotFound {
		t.Fatalf("source delete error: got %v, expected %v", err, chronograf.ErrSourceNotFound)
	}

	// Delete the other source we created
	if err := s.Delete(ctx, srcs[3]); err != nil {
		t.Fatal(err)
	}

	if bsrcs, err := s.All(ctx); err != nil {
		t.Fatal(err)
	} else if len(bsrcs) != 3 {
		t.Fatalf("After delete All returned incorrect number of srcs; got %d, expected %d", len(bsrcs), 3)
	} else if !reflect.DeepEqual(bsrcs[0], srcs[1]) {
		t.Fatalf("After delete All returned incorrect source; got %v, expected %v", bsrcs[0], srcs[1])
	}

	// Delete the final sources
	if err := s.Delete(ctx, srcs[1]); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, srcs[2]); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, *bolt.DefaultSource); err != nil {
		t.Fatal(err)
	}

	// Try to add one source as a non-default and ensure that it becomes a
	// default
	src := mustAddSource(t, s, chronograf.Source{
		Name:         "Biff Tannen",
		Type:         "influx",
		Username:     "HELLO",
		Password:     "MCFLY",
		URL:          "anybody.in.there.local",
		Default:      false,
		Organization: "1234",
	})

	if actual, err := s.Get(ctx, src.ID); err != nil {
		t.Fatal(err)
	} else if !actual.Default {
		t.Fatal("Expected first source added to be default but wasn't")
	}
}

func mustUpdateSource(t *testing.T, s *bolt.SourcesStore, src chronograf.Source) {
	ctx := context.Background()
	if err := s.Update(ctx, src); err != nil {
		t.Fatal(err)
	}
}

func mustAddSource(t *testing.T, s *bolt.SourcesStore, src chronograf.Source) chronograf.Source {
	ctx := context.Background()
	if src, err := s.Add(ctx, src); err != nil {
		t.Fatal(err)
		return src
	} else {
		return src
	}
}
