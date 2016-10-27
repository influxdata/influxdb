package tsi1_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsi1"
)

// Ensure iterator can operate over an in-memory list of elements.
func TestMeasurementIterator(t *testing.T) {
	elems := []tsi1.MeasurementElem{
		{Name: []byte("cpu"), Deleted: true},
		{Name: []byte("mem")},
	}

	itr := tsi1.NewMeasurementIterator(elems)
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
		tsi1.NewMeasurementIterator([]tsi1.MeasurementElem{
			{Name: []byte("aaa")},
			{Name: []byte("bbb"), Deleted: true},
			{Name: []byte("ccc")},
		}),
		tsi1.NewMeasurementIterator(nil),
		tsi1.NewMeasurementIterator([]tsi1.MeasurementElem{
			{Name: []byte("bbb")},
			{Name: []byte("ccc"), Deleted: true},
			{Name: []byte("ddd")},
		}),
	)

	if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.MeasurementElem{Name: []byte("aaa")}) {
		t.Fatalf("unexpected elem(0): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.MeasurementElem{Name: []byte("bbb"), Deleted: true}) {
		t.Fatalf("unexpected elem(1): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.MeasurementElem{Name: []byte("ccc")}) {
		t.Fatalf("unexpected elem(2): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.MeasurementElem{Name: []byte("ddd")}) {
		t.Fatalf("unexpected elem(3): %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can operate over an in-memory list of tag key elements.
func TestTagKeyIterator(t *testing.T) {
	elems := []tsi1.TagKeyElem{
		{Key: []byte("aaa"), Deleted: true},
		{Key: []byte("bbb")},
	}

	itr := tsi1.NewTagKeyIterator(elems)
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
		tsi1.NewTagKeyIterator([]tsi1.TagKeyElem{
			{Key: []byte("aaa")},
			{Key: []byte("bbb"), Deleted: true},
			{Key: []byte("ccc")},
		}),
		tsi1.NewTagKeyIterator(nil),
		tsi1.NewTagKeyIterator([]tsi1.TagKeyElem{
			{Key: []byte("bbb")},
			{Key: []byte("ccc"), Deleted: true},
			{Key: []byte("ddd")},
		}),
	)

	if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.TagKeyElem{Key: []byte("aaa")}) {
		t.Fatalf("unexpected elem(0): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.TagKeyElem{Key: []byte("bbb"), Deleted: true}) {
		t.Fatalf("unexpected elem(1): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.TagKeyElem{Key: []byte("ccc")}) {
		t.Fatalf("unexpected elem(2): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.TagKeyElem{Key: []byte("ddd")}) {
		t.Fatalf("unexpected elem(3): %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can operate over an in-memory list of tag value elements.
func TestTagValueIterator(t *testing.T) {
	elems := []tsi1.TagValueElem{
		{Value: []byte("aaa"), Deleted: true},
		{Value: []byte("bbb")},
	}

	itr := tsi1.NewTagValueIterator(elems)
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
		tsi1.NewTagValueIterator([]tsi1.TagValueElem{
			{Value: []byte("aaa")},
			{Value: []byte("bbb"), Deleted: true},
			{Value: []byte("ccc")},
		}),
		tsi1.NewTagValueIterator(nil),
		tsi1.NewTagValueIterator([]tsi1.TagValueElem{
			{Value: []byte("bbb")},
			{Value: []byte("ccc"), Deleted: true},
			{Value: []byte("ddd")},
		}),
	)

	if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.TagValueElem{Value: []byte("aaa")}) {
		t.Fatalf("unexpected elem(0): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.TagValueElem{Value: []byte("bbb"), Deleted: true}) {
		t.Fatalf("unexpected elem(1): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.TagValueElem{Value: []byte("ccc")}) {
		t.Fatalf("unexpected elem(2): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.TagValueElem{Value: []byte("ddd")}) {
		t.Fatalf("unexpected elem(3): %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}

// Ensure iterator can operate over an in-memory list of series.
func TestSeriesIterator(t *testing.T) {
	elems := []tsi1.SeriesElem{
		{Name: []byte("cpu"), Tags: models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}, Deleted: true},
		{Name: []byte("mem")},
	}

	itr := tsi1.NewSeriesIterator(elems)
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
		tsi1.NewSeriesIterator([]tsi1.SeriesElem{
			{Name: []byte("aaa"), Tags: models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}, Deleted: true},
			{Name: []byte("bbb"), Deleted: true},
			{Name: []byte("ccc")},
		}),
		tsi1.NewSeriesIterator(nil),
		tsi1.NewSeriesIterator([]tsi1.SeriesElem{
			{Name: []byte("aaa"), Tags: models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}},
			{Name: []byte("aaa"), Tags: models.Tags{{Key: []byte("region"), Value: []byte("us-west")}}},
			{Name: []byte("bbb")},
			{Name: []byte("ccc"), Deleted: true},
			{Name: []byte("ddd")},
		}),
	)

	if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.SeriesElem{Name: []byte("aaa"), Tags: models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}, Deleted: true}) {
		t.Fatalf("unexpected elem(0): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.SeriesElem{Name: []byte("aaa"), Tags: models.Tags{{Key: []byte("region"), Value: []byte("us-west")}}}) {
		t.Fatalf("unexpected elem(1): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.SeriesElem{Name: []byte("bbb"), Deleted: true}) {
		t.Fatalf("unexpected elem(2): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.SeriesElem{Name: []byte("ccc")}) {
		t.Fatalf("unexpected elem(3): %#v", e)
	} else if e := itr.Next(); !reflect.DeepEqual(e, &tsi1.SeriesElem{Name: []byte("ddd")}) {
		t.Fatalf("unexpected elem(4): %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected nil elem: %#v", e)
	}
}
