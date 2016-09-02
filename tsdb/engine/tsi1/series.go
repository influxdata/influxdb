package tsi1

import (
	"io"
)

//
type SeriesDictionary struct{}
type Dictionary struct{}
type SeriesList struct{}

type SeriesWriter struct {
	w io.Writer
}
