package server

import "testing"

func TestLayoutBuilder(t *testing.T) {
	var l LayoutBuilder = &MultiLayoutBuilder{}
	layout, err := l.Build(nil)
	if err != nil {
		t.Fatalf("MultiLayoutBuilder can't build a MultiLayoutStore: %v", err)
	}

	if layout == nil {
		t.Fatal("LayoutBuilder should have built a layout")
	}
}

func TestSourcesStoresBuilder(t *testing.T) {
	var b SourcesBuilder = &MultiSourceBuilder{}
	sources, err := b.Build(nil)
	if err != nil {
		t.Fatalf("MultiSourceBuilder can't build a MultiSourcesStore: %v", err)
	}
	if sources == nil {
		t.Fatal("SourcesBuilder should have built a MultiSourceStore")
	}
}
