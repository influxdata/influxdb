package memdb

import (
	"context"
	"testing"

	"github.com/influxdata/platform/chronograf"
)

func TestInterfaceImplementation(t *testing.T) {
	var _ chronograf.ServersStore = &KapacitorStore{}
}

func TestKapacitorStoreAll(t *testing.T) {
	ctx := context.Background()

	store := KapacitorStore{}
	kaps, err := store.All(ctx)
	if err != nil {
		t.Fatal("All should not throw an error with an empty Store")
	}
	if len(kaps) != 0 {
		t.Fatal("Store should be empty")
	}

	store.Kapacitor = &chronograf.Server{}
	kaps, err = store.All(ctx)
	if err != nil {
		t.Fatal("All should not throw an error with an empty Store")
	}
	if len(kaps) != 1 {
		t.Fatal("Store should have 1 element")
	}
}

func TestKapacitorStoreAdd(t *testing.T) {
	ctx := context.Background()

	store := KapacitorStore{}
	_, err := store.Add(ctx, chronograf.Server{})
	if err == nil {
		t.Fatal("Store should not support adding another source")
	}
}

func TestKapacitorStoreDelete(t *testing.T) {
	ctx := context.Background()

	store := KapacitorStore{}
	err := store.Delete(ctx, chronograf.Server{})
	if err == nil {
		t.Fatal("Delete should not operate on an empty Store")
	}

	store.Kapacitor = &chronograf.Server{
		ID: 9,
	}
	err = store.Delete(ctx, chronograf.Server{
		ID: 8,
	})
	if err == nil {
		t.Fatal("Delete should not remove elements with the wrong ID")
	}

	err = store.Delete(ctx, chronograf.Server{
		ID: 9,
	})
	if err != nil {
		t.Fatal("Delete should remove an element with a matching ID")
	}
}

func TestKapacitorStoreGet(t *testing.T) {
	ctx := context.Background()

	store := KapacitorStore{}
	_, err := store.Get(ctx, 9)
	if err == nil {
		t.Fatal("Get should return an error for an empty Store")
	}

	store.Kapacitor = &chronograf.Server{
		ID: 9,
	}
	_, err = store.Get(ctx, 8)
	if err == nil {
		t.Fatal("Get should return an error if it finds no matches")
	}

	store.Kapacitor = &chronograf.Server{
		ID: 9,
	}
	kap, err := store.Get(ctx, 9)
	if err != nil || kap.ID != 9 {
		t.Fatal("Get should find the element with a matching ID")
	}
}

func TestKapacitorStoreUpdate(t *testing.T) {
	ctx := context.Background()

	store := KapacitorStore{}
	err := store.Update(ctx, chronograf.Server{})
	if err == nil {
		t.Fatal("Update fhouls return an error for an empty Store")
	}

	store.Kapacitor = &chronograf.Server{
		ID: 9,
	}
	err = store.Update(ctx, chronograf.Server{
		ID: 8,
	})
	if err == nil {
		t.Fatal("Update should return an error if it finds no matches")
	}

	store.Kapacitor = &chronograf.Server{
		ID: 9,
	}
	err = store.Update(ctx, chronograf.Server{
		ID:  9,
		URL: "http://crystal.pepsi.com",
	})
	if err != nil || store.Kapacitor.URL != "http://crystal.pepsi.com" {
		t.Fatal("Update should overwrite elements with matching IDs")
	}
}
