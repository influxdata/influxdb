package tsi1

import (
	"io"
)

// Index represents a collection of measurement, tag, and series data.
type Index struct {
	// Filename of index file.
	Path string

	// Memory-map references.
	data []byte
}

// Open opens and initializes an index file.
func (i *Index) Open() error { panic("TODO") }

// Close closes the index file.
func (i *Index) Close() error { panic("TODO") }

// Indices represents a layered set of indices.
type Indices []*Index

// IndexWriter represents a writer for building indexes.
type IndexWriter struct {
	w io.Writer
}
