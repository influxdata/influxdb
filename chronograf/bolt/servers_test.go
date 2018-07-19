package bolt_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/influxdata/platform/chronograf"
)

// Ensure an ServerStore can store, retrieve, update, and delete servers.
func TestServerStore(t *testing.T) {
	c, err := NewTestClient()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	s := c.ServersStore

	srcs := []chronograf.Server{
		chronograf.Server{
			Name:               "Of Truth",
			SrcID:              10,
			Username:           "marty",
			Password:           "I❤️  jennifer parker",
			URL:                "toyota-hilux.lyon-estates.local",
			Active:             false,
			Organization:       "133",
			InsecureSkipVerify: true,
		},
		chronograf.Server{
			Name:               "HipToBeSquare",
			SrcID:              12,
			Username:           "calvinklein",
			Password:           "chuck b3rry",
			URL:                "toyota-hilux.lyon-estates.local",
			Active:             false,
			Organization:       "133",
			InsecureSkipVerify: false,
		},
	}

	// Add new srcs.
	ctx := context.Background()
	for i, src := range srcs {
		if srcs[i], err = s.Add(ctx, src); err != nil {
			t.Fatal(err)
		}
		// Confirm first src in the store is the same as the original.
		if actual, err := s.Get(ctx, srcs[i].ID); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(actual, srcs[i]) {
			t.Fatalf("server loaded is different then server saved; actual: %v, expected %v", actual, srcs[i])
		}
	}

	// Update server.
	srcs[0].Username = "calvinklein"
	srcs[1].Name = "Enchantment Under the Sea Dance"
	srcs[1].Organization = "1234"
	if err := s.Update(ctx, srcs[0]); err != nil {
		t.Fatal(err)
	} else if err := s.Update(ctx, srcs[1]); err != nil {
		t.Fatal(err)
	}

	// Confirm servers have updated.
	if src, err := s.Get(ctx, srcs[0].ID); err != nil {
		t.Fatal(err)
	} else if src.Username != "calvinklein" {
		t.Fatalf("server 0 update error: got %v, expected %v", src.Username, "calvinklein")
	}
	if src, err := s.Get(ctx, srcs[1].ID); err != nil {
		t.Fatal(err)
	} else if src.Name != "Enchantment Under the Sea Dance" {
		t.Fatalf("server 1 update error: got %v, expected %v", src.Name, "Enchantment Under the Sea Dance")
	} else if src.Organization != "1234" {
		t.Fatalf("server 1 update error: got %v, expected %v", src.Organization, "1234")
	}

	// Attempt to make two active sources
	srcs[0].Active = true
	srcs[1].Active = true
	if err := s.Update(ctx, srcs[0]); err != nil {
		t.Fatal(err)
	} else if err := s.Update(ctx, srcs[1]); err != nil {
		t.Fatal(err)
	}

	if actual, err := s.Get(ctx, srcs[0].ID); err != nil {
		t.Fatal(err)
	} else if actual.Active == true {
		t.Fatal("Able to set two active servers when only one should be permitted")
	}

	// Delete an server.
	if err := s.Delete(ctx, srcs[0]); err != nil {
		t.Fatal(err)
	}

	// Confirm server has been deleted.
	if _, err := s.Get(ctx, srcs[0].ID); err != chronograf.ErrServerNotFound {
		t.Fatalf("server delete error: got %v, expected %v", err, chronograf.ErrServerNotFound)
	}

	if bsrcs, err := s.All(ctx); err != nil {
		t.Fatal(err)
	} else if len(bsrcs) != 1 {
		t.Fatalf("After delete All returned incorrect number of srcs; got %d, expected %d", len(bsrcs), 1)
	} else if !reflect.DeepEqual(bsrcs[0], srcs[1]) {
		t.Fatalf("After delete All returned incorrect server; got %v, expected %v", bsrcs[0], srcs[1])
	}
}
