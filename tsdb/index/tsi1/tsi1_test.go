package tsi1_test

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"github.com/influxdata/influxql"
)

// Ensure iterator can operate over an in-memory list of elements.
func TestMeasurementIterator(t *testing.T) {
	elems := []MeasurementElem{
		MeasurementElem{name: []byte("cpu"), deleted: true},
		MeasurementElem{name: []byte("mem")},
	}

	itr := MeasurementIterator{Elems: elems}
	if e := itr.Next(); !reflect.DeepEqual(&elems[0], e) {
		t.Fatalf("unexpected elem(0): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(&elems[1], e) {
		t.Fatalf("unexpected elem(1): %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can merge multiple iterators together.
func TestMergeMeasurementIterators(t *testing.T) {
	itr := tsi1.MergeMeasurementIterators(
		&MeasurementIterator{Elems: []MeasurementElem{
			{name: []byte("aaa")},
			{name: []byte("bbb"), deleted: true},
			{name: []byte("ccc")},
		}},
		&MeasurementIterator{},
		&MeasurementIterator{Elems: []MeasurementElem{
			{name: []byte("bbb")},
			{name: []byte("ccc"), deleted: true},
			{name: []byte("ddd")},
		}},
	)

	if e := itr.Next(); !bytes.Equal(e.Name(), []byte("aaa")) || e.Deleted() {
		t.Fatalf("unexpected elem(0): %s/%v", e.Name(), e.Deleted())
	} else if e := itr.Next(); !bytes.Equal(e.Name(), []byte("bbb")) || !e.Deleted() {
		t.Fatalf("unexpected elem(1): %s/%v", e.Name(), e.Deleted())
	} else if e := itr.Next(); !bytes.Equal(e.Name(), []byte("ccc")) || e.Deleted() {
		t.Fatalf("unexpected elem(2): %s/%v", e.Name(), e.Deleted())
	} else if e := itr.Next(); !bytes.Equal(e.Name(), []byte("ddd")) || e.Deleted() {
		t.Fatalf("unexpected elem(3): %s/%v", e.Name(), e.Deleted())
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can operate over an in-memory list of tag key elements.
func TestTagKeyIterator(t *testing.T) {
	elems := []TagKeyElem{
		{key: []byte("aaa"), deleted: true},
		{key: []byte("bbb")},
	}

	itr := TagKeyIterator{Elems: elems}
	if e := itr.Next(); !reflect.DeepEqual(&elems[0], e) {
		t.Fatalf("unexpected elem(0): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(&elems[1], e) {
		t.Fatalf("unexpected elem(1): %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can merge multiple iterators together.
func TestMergeTagKeyIterators(t *testing.T) {
	itr := tsi1.MergeTagKeyIterators(
		&TagKeyIterator{Elems: []TagKeyElem{
			{key: []byte("aaa")},
			{key: []byte("bbb"), deleted: true},
			{key: []byte("ccc")},
		}},
		&TagKeyIterator{},
		&TagKeyIterator{Elems: []TagKeyElem{
			{key: []byte("bbb")},
			{key: []byte("ccc"), deleted: true},
			{key: []byte("ddd")},
		}},
	)

	if e := itr.Next(); !bytes.Equal(e.Key(), []byte("aaa")) || e.Deleted() {
		t.Fatalf("unexpected elem(0): %s/%v", e.Key(), e.Deleted())
	} else if e := itr.Next(); !bytes.Equal(e.Key(), []byte("bbb")) || !e.Deleted() {
		t.Fatalf("unexpected elem(1): %s/%v", e.Key(), e.Deleted())
	} else if e := itr.Next(); !bytes.Equal(e.Key(), []byte("ccc")) || e.Deleted() {
		t.Fatalf("unexpected elem(2): %s/%v", e.Key(), e.Deleted())
	} else if e := itr.Next(); !bytes.Equal(e.Key(), []byte("ddd")) || e.Deleted() {
		t.Fatalf("unexpected elem(3): %s/%v", e.Key(), e.Deleted())
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can operate over an in-memory list of tag value elements.
func TestTagValueIterator(t *testing.T) {
	elems := []TagValueElem{
		{value: []byte("aaa"), deleted: true},
		{value: []byte("bbb")},
	}

	itr := &TagValueIterator{Elems: elems}
	if e := itr.Next(); !reflect.DeepEqual(&elems[0], e) {
		t.Fatalf("unexpected elem(0): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(&elems[1], e) {
		t.Fatalf("unexpected elem(1): %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can merge multiple iterators together.
func TestMergeTagValueIterators(t *testing.T) {
	itr := tsi1.MergeTagValueIterators(
		&TagValueIterator{Elems: []TagValueElem{
			{value: []byte("aaa")},
			{value: []byte("bbb"), deleted: true},
			{value: []byte("ccc")},
		}},
		&TagValueIterator{},
		&TagValueIterator{Elems: []TagValueElem{
			{value: []byte("bbb")},
			{value: []byte("ccc"), deleted: true},
			{value: []byte("ddd")},
		}},
	)

	if e := itr.Next(); !bytes.Equal(e.Value(), []byte("aaa")) || e.Deleted() {
		t.Fatalf("unexpected elem(0): %s/%v", e.Value(), e.Deleted())
	} else if e := itr.Next(); !bytes.Equal(e.Value(), []byte("bbb")) || !e.Deleted() {
		t.Fatalf("unexpected elem(1): %s/%v", e.Value(), e.Deleted())
	} else if e := itr.Next(); !bytes.Equal(e.Value(), []byte("ccc")) || e.Deleted() {
		t.Fatalf("unexpected elem(2): %s/%v", e.Value(), e.Deleted())
	} else if e := itr.Next(); !bytes.Equal(e.Value(), []byte("ddd")) || e.Deleted() {
		t.Fatalf("unexpected elem(3): %s/%v", e.Value(), e.Deleted())
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can operate over an in-memory list of series.
func TestSeriesIterator(t *testing.T) {
	elems := []SeriesElem{
		{name: []byte("cpu"), tags: models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}, deleted: true},
		{name: []byte("mem")},
	}

	itr := SeriesIterator{Elems: elems}
	if e := itr.Next(); !reflect.DeepEqual(&elems[0], e) {
		t.Fatalf("unexpected elem(0): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(&elems[1], e) {
		t.Fatalf("unexpected elem(1): %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can merge multiple iterators together.
func TestMergeSeriesIterators(t *testing.T) {
	itr := tsi1.MergeSeriesIterators(
		&SeriesIterator{Elems: []SeriesElem{
			{name: []byte("aaa"), tags: models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}, deleted: true},
			{name: []byte("bbb"), deleted: true},
			{name: []byte("ccc")},
		}},
		&SeriesIterator{},
		&SeriesIterator{Elems: []SeriesElem{
			{name: []byte("aaa"), tags: models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}},
			{name: []byte("aaa"), tags: models.Tags{{Key: []byte("region"), Value: []byte("us-west")}}},
			{name: []byte("bbb")},
			{name: []byte("ccc"), deleted: true},
			{name: []byte("ddd")},
		}},
	)

	if e := itr.Next(); !reflect.DeepEqual(e, &SeriesElem{name: []byte("aaa"), tags: models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}, deleted: true}) {
		t.Fatalf("unexpected elem(0): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &SeriesElem{name: []byte("aaa"), tags: models.Tags{{Key: []byte("region"), Value: []byte("us-west")}}}) {
		t.Fatalf("unexpected elem(1): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &SeriesElem{name: []byte("bbb"), deleted: true}) {
		t.Fatalf("unexpected elem(2): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &SeriesElem{name: []byte("ccc")}) {
		t.Fatalf("unexpected elem(3): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &SeriesElem{name: []byte("ddd")}) {
		t.Fatalf("unexpected elem(4): %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// MeasurementElem represents a test implementation of tsi1.MeasurementElem.
type MeasurementElem struct {
	name    []byte
	deleted bool
}

func (e *MeasurementElem) Name() []byte                        { return e.name }
func (e *MeasurementElem) Deleted() bool                       { return e.deleted }
func (e *MeasurementElem) TagKeyIterator() tsi1.TagKeyIterator { return nil }

// MeasurementIterator represents an iterator over a slice of measurements.
type MeasurementIterator struct {
	Elems []MeasurementElem
}

// Next returns the next element in the iterator.
func (itr *MeasurementIterator) Next() (e tsi1.MeasurementElem) {
	if len(itr.Elems) == 0 {
		return nil
	}
	e, itr.Elems = &itr.Elems[0], itr.Elems[1:]
	return e
}

// TagKeyElem represents a test implementation of tsi1.TagKeyElem.
type TagKeyElem struct {
	key     []byte
	deleted bool
}

func (e *TagKeyElem) Key() []byte                             { return e.key }
func (e *TagKeyElem) Deleted() bool                           { return e.deleted }
func (e *TagKeyElem) TagValueIterator() tsi1.TagValueIterator { return nil }

// TagKeyIterator represents an iterator over a slice of tag keys.
type TagKeyIterator struct {
	Elems []TagKeyElem
}

// Next returns the next element in the iterator.
func (itr *TagKeyIterator) Next() (e tsi1.TagKeyElem) {
	if len(itr.Elems) == 0 {
		return nil
	}
	e, itr.Elems = &itr.Elems[0], itr.Elems[1:]
	return e
}

// TagValueElem represents a test implementation of tsi1.TagValueElem.
type TagValueElem struct {
	value   []byte
	deleted bool
}

func (e *TagValueElem) Value() []byte                       { return e.value }
func (e *TagValueElem) Deleted() bool                       { return e.deleted }
func (e *TagValueElem) SeriesIterator() tsi1.SeriesIterator { return nil }

// TagValueIterator represents an iterator over a slice of tag values.
type TagValueIterator struct {
	Elems []TagValueElem
}

// Next returns the next element in the iterator.
func (itr *TagValueIterator) Next() (e tsi1.TagValueElem) {
	if len(itr.Elems) == 0 {
		return nil
	}
	e, itr.Elems = &itr.Elems[0], itr.Elems[1:]
	return e
}

// SeriesElem represents a test implementation of tsi1.SeriesElem.
type SeriesElem struct {
	name    []byte
	tags    models.Tags
	deleted bool
	expr    influxql.Expr
}

func (e *SeriesElem) Name() []byte        { return e.name }
func (e *SeriesElem) Tags() models.Tags   { return e.tags }
func (e *SeriesElem) Deleted() bool       { return e.deleted }
func (e *SeriesElem) Expr() influxql.Expr { return e.expr }

// SeriesIterator represents an iterator over a slice of tag values.
type SeriesIterator struct {
	Elems []SeriesElem
}

// Next returns the next element in the iterator.
func (itr *SeriesIterator) Next() (e tsi1.SeriesElem) {
	if len(itr.Elems) == 0 {
		return nil
	}
	e, itr.Elems = &itr.Elems[0], itr.Elems[1:]
	return e
}

// MustTempDir returns a temporary directory. Panic on error.
func MustTempDir() string {
	path, err := ioutil.TempDir("", "tsi-")
	if err != nil {
		panic(err)
	}
	return path
}
