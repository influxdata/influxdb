package storage_test

//WritePoints does nothing in error state
//the main WritePoints scenarios (large write, etc)

import (
	"context"
	"errors"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/tsdb"
)

func TestBufferedPointsWriter(t *testing.T) {
	t.Run("large empty write on empty buffer", func(t *testing.T) {
		pw := &mock.PointsWriter{}
		bpw := storage.NewBufferedPointsWriter(6, pw)
		bpw.WritePoints(
			context.Background(),
			mockPoints(
				1,
				2,
				`a day="Monday",humidity=1,ratio=2,temperature=2 11
a day="Tuesday",humidity=2,ratio=1,temperature=2 21
b day="Wednesday",humidity=4,ratio=0.25,temperature=1 21
a day="Thursday",humidity=3,ratio=1,temperature=3 31
c day="Friday",humidity=5,ratio=0,temperature=4 41
e day="Saturday",humidity=6,ratio=0.1,temperature=99 51
`))

		if pw.Err != nil {
			t.Error(pw.Err)
		}
		if len(pw.Points) != 24 {
			t.Errorf("long writes on empty buffer should write all points but only wrote %d", len(pw.Points))
		}
		if pw.WritePointsCalled() != 1 {
			t.Errorf("expected WritePoints to be called once, but was called %d times", pw.WritePointsCalled())
		}
	})
	t.Run("do nothing in error state", func(t *testing.T) {
		pw := &mock.PointsWriter{}
		bpw := storage.NewBufferedPointsWriter(6, pw)
		bpw.WritePoints(
			context.Background(),
			mockPoints(
				1,
				2,
				`a day="Monday",humidity=1,ratio=2,temperature=2 11
`))
		pw.ForceError(errors.New("OH NO! ERRORZ!"))
		err := bpw.WritePoints(
			context.Background(),
			mockPoints(
				1,
				2,
				`a day="Tuesday",humidity=2,ratio=1,temperature=2 21
b day="Wednesday",humidity=4,ratio=0.25,temperature=1 21
a day="Thursday",humidity=3,ratio=1,temperature=3 31
c day="Friday",humidity=5,ratio=0,temperature=4 41
e day="Saturday",humidity=6,ratio=0.1,temperature=99 51
`))
		if pw.Err != err {
			t.Error("expected the error returned to be the forced one, but it was not")
		}
		if pw.WritePointsCalled() != 1 {
			t.Errorf("expected WritePoints to be called once, since it should do nothing in the error state, but was called %d times", pw.WritePointsCalled())
		}

	})
	t.Run("flush on write when over limit", func(t *testing.T) {
		pw := &mock.PointsWriter{}
		bpw := storage.NewBufferedPointsWriter(6, pw)
		bpw.WritePoints(context.Background(), mockPoints(1, 2, `a day="Monday",humidity=1,ratio=2,temperature=2 11`))
		bpw.WritePoints(context.Background(), mockPoints(1, 2, `a day="Tuesday",humidity=2,ratio=1,temperature=2 21`))
		bpw.WritePoints(context.Background(), mockPoints(1, 2, `b day="Wednesday",humidity=4,ratio=0.25,temperature=1 21`))
		bpw.WritePoints(context.Background(), mockPoints(1, 2, `a day="Thursday",humidity=3,ratio=1,temperature=3 31`))
		bpw.WritePoints(context.Background(), mockPoints(1, 2, `c day="Friday",humidity=5,ratio=0,temperature=4 41`))
		bpw.WritePoints(context.Background(), mockPoints(1, 2, `e day="Saturday",humidity=6,ratio=0.1,temperature=99 51`))
		if pw.Err != nil {
			t.Errorf("expected no error, but got %v", pw.Err)
		}
		if pw.WritePointsCalled() != 3 {
			t.Errorf("expected WritePoints to be called 3 times, but was called %d times", pw.WritePointsCalled())
		}

		bpw.Flush(context.Background())
		if pw.WritePointsCalled() != 4 {
			t.Errorf("expected WritePoints to be called 4 times, but was called %d times", pw.WritePointsCalled())
		}

		bpw.Flush(context.Background())
		if pw.WritePointsCalled() != 4 {
			t.Errorf("expected WritePoints to be called 4 times, but was called %d times", pw.WritePointsCalled())
		}
	})

	t.Run("don't flush when empty", func(t *testing.T) {
		pw := &mock.PointsWriter{}
		bpw := storage.NewBufferedPointsWriter(6, pw)
		bpw.Flush(context.Background())
		if pw.WritePointsCalled() != 0 {
			t.Errorf("expected WritePoints to not be falled but was called %d times", pw.WritePointsCalled())
		}
	})
}

func mockPoints(org, bucket influxdb.ID, pointdata string) []models.Point {
	name := tsdb.EncodeName(org, bucket)
	points, err := models.ParsePoints([]byte(pointdata), name[:])
	if err != nil {
		panic(err)
	}
	return points
}
