package mock

import (
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/influxql"
)

// Iterators is a test wrapper for iterators.
type Iterators []influxql.Iterator

// Next returns the next value from each iterator.
// Returns nil if any iterator returns a nil.
func (itrs Iterators) Next() ([]influxql.Point, error) {
	a := make([]influxql.Point, len(itrs))
	for i, itr := range itrs {
		switch itr := itr.(type) {
		case influxql.FloatIterator:
			fp, err := itr.Next()
			if fp == nil || err != nil {
				return nil, err
			}
			a[i] = fp
		case influxql.IntegerIterator:
			ip, err := itr.Next()
			if ip == nil || err != nil {
				return nil, err
			}
			a[i] = ip
		case influxql.StringIterator:
			sp, err := itr.Next()
			if sp == nil || err != nil {
				return nil, err
			}
			a[i] = sp
		case influxql.BooleanIterator:
			bp, err := itr.Next()
			if bp == nil || err != nil {
				return nil, err
			}
			a[i] = bp
		default:
			panic(fmt.Sprintf("iterator type not supported: %T", itr))
		}
	}
	return a, nil
}

// ReadAll reads all points from all iterators.
func (itrs Iterators) ReadAll() ([][]influxql.Point, error) {
	var a [][]influxql.Point

	// Read from every iterator until a nil is encountered.
	for {
		points, err := itrs.Next()
		if err != nil {
			return nil, err
		} else if points == nil {
			break
		}
		a = append(a, influxql.Points(points).Clone())
	}

	// Close all iterators.
	influxql.Iterators(itrs).Close()

	return a, nil
}

// Test implementation of influxql.FloatIterator
type FloatIterator struct {
	Points []influxql.FloatPoint
	Closed bool
	stats  influxql.IteratorStats
}

func (itr *FloatIterator) Stats() influxql.IteratorStats { return itr.stats }
func (itr *FloatIterator) Close() error                  { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *FloatIterator) Next() (*influxql.FloatPoint, error) {
	if len(itr.Points) == 0 || itr.Closed {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v, nil
}

// Test implementation of influxql.IntegerIterator
type IntegerIterator struct {
	Points []influxql.IntegerPoint
	Closed bool
	stats  influxql.IteratorStats
}

func (itr *IntegerIterator) Stats() influxql.IteratorStats { return itr.stats }
func (itr *IntegerIterator) Close() error                  { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *IntegerIterator) Next() (*influxql.IntegerPoint, error) {
	if len(itr.Points) == 0 || itr.Closed {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v, nil
}

// Test implementation of influxql.StringIterator
type StringIterator struct {
	Points []influxql.StringPoint
	Closed bool
	stats  influxql.IteratorStats
}

func (itr *StringIterator) Stats() influxql.IteratorStats { return itr.stats }
func (itr *StringIterator) Close() error                  { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *StringIterator) Next() (*influxql.StringPoint, error) {
	if len(itr.Points) == 0 || itr.Closed {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v, nil
}

// Test implementation of influxql.BooleanIterator
type BooleanIterator struct {
	Points []influxql.BooleanPoint
	Closed bool
	stats  influxql.IteratorStats
}

func (itr *BooleanIterator) Stats() influxql.IteratorStats { return itr.stats }
func (itr *BooleanIterator) Close() error                  { itr.Closed = true; return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *BooleanIterator) Next() (*influxql.BooleanPoint, error) {
	if len(itr.Points) == 0 || itr.Closed {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v, nil
}

type FloatPointGenerator struct {
	i  int
	N  int
	Fn func(i int) *influxql.FloatPoint
}

func (g *FloatPointGenerator) Close() error                  { return nil }
func (g *FloatPointGenerator) Stats() influxql.IteratorStats { return influxql.IteratorStats{} }

func (g *FloatPointGenerator) Next() (*influxql.FloatPoint, error) {
	if g.i == g.N {
		return nil, nil
	}
	p := g.Fn(g.i)
	g.i++
	return p, nil
}

// ParseTags returns an instance of Tags for a comma-delimited list of key/values.
func ParseTags(s string) influxql.Tags {
	m := make(map[string]string)
	for _, kv := range strings.Split(s, ",") {
		a := strings.Split(kv, "=")
		m[a[0]] = a[1]
	}
	return influxql.NewTags(m)
}
