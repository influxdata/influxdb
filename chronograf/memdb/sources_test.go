package memdb

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/chronograf"
)

func TestSourcesStore(t *testing.T) {
	var _ chronograf.SourcesStore = &SourcesStore{}
}

func TestSourcesStoreAdd(t *testing.T) {
	ctx := context.Background()

	store := SourcesStore{}
	_, err := store.Add(ctx, chronograf.Source{})
	if err == nil {
		t.Fatal("Store should not support adding another source")
	}
}

func TestSourcesStoreAll(t *testing.T) {
	ctx := context.Background()

	store := SourcesStore{}
	srcs, err := store.All(ctx)
	if err != nil {
		t.Fatal("All should not throw an error with an empty Store")
	}
	if len(srcs) != 0 {
		t.Fatal("Store should be empty")
	}

	store.Source = &chronograf.Source{}
	srcs, err = store.All(ctx)
	if err != nil {
		t.Fatal("All should not throw an error with an empty Store")
	}
	if len(srcs) != 1 {
		t.Fatal("Store should have 1 element")
	}
}

func TestSourcesStoreDelete(t *testing.T) {
	ctx := context.Background()

	store := SourcesStore{}
	err := store.Delete(ctx, chronograf.Source{})
	if err == nil {
		t.Fatal("Delete should not operate on an empty Store")
	}

	store.Source = &chronograf.Source{
		ID: 9,
	}
	err = store.Delete(ctx, chronograf.Source{
		ID: 8,
	})
	if err == nil {
		t.Fatal("Delete should not remove elements with the wrong ID")
	}

	err = store.Delete(ctx, chronograf.Source{
		ID: 9,
	})
	if err != nil {
		t.Fatal("Delete should remove an element with a matching ID")
	}
}

func TestSourcesStoreGet(t *testing.T) {
	ctx := context.Background()

	store := SourcesStore{}
	_, err := store.Get(ctx, 9)
	if err == nil {
		t.Fatal("Get should return an error for an empty Store")
	}

	store.Source = &chronograf.Source{
		ID: 9,
	}
	_, err = store.Get(ctx, 8)
	if err == nil {
		t.Fatal("Get should return an error if it finds no matches")
	}

	store.Source = &chronograf.Source{
		ID: 9,
	}
	src, err := store.Get(ctx, 9)
	if err != nil || src.ID != 9 {
		t.Fatal("Get should find the element with a matching ID")
	}
}

func TestSourcesStoreUpdate(t *testing.T) {
	ctx := context.Background()

	store := SourcesStore{}
	err := store.Update(ctx, chronograf.Source{})
	if err == nil {
		t.Fatal("Update should return an error for an empty Store")
	}

	store.Source = &chronograf.Source{
		ID: 9,
	}
	err = store.Update(ctx, chronograf.Source{
		ID: 8,
	})
	if err == nil {
		t.Fatal("Update should return an error if it finds no matches")
	}

	store.Source = &chronograf.Source{
		ID: 9,
	}
	err = store.Update(ctx, chronograf.Source{
		ID:  9,
		URL: "http://crystal.pepsi.com",
	})
	if err != nil || store.Source.URL != "http://crystal.pepsi.com" {
		t.Fatal("Update should overwrite elements with matching IDs")
	}
}
