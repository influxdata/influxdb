package storage_test

//WritePoints does nothing in error state
//the main WritePoints scenarios (large write, etc)

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/tsdb"
)

func TestLoggingPointsWriter(t *testing.T) {
	// Ensure a successful write will not be logged.
	t.Run("OK", func(t *testing.T) {
		var n int
		lpw := &storage.LoggingPointsWriter{
			Underlying: &mock.PointsWriter{
				WritePointsFn: func(ctx context.Context, p []models.Point) error {
					switch n++; n {
					case 1:
						return nil
					default:
						t.Fatal("too many calls to WritePoints()")
						return nil
					}
				},
			},
		}

		if err := lpw.WritePoints(context.Background(), []models.Point{models.MustNewPoint(
			tsdb.EncodeNameString(1, 2),
			models.NewTags(map[string]string{"t": "v"}),
			models.Fields{"f": float64(100)},
			time.Now(),
		)}); err != nil {
			t.Fatal(err)
		} else if got, want := n, 1; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}
	})

	// Ensure an errored write will be logged afterward.
	t.Run("ErroredWrite", func(t *testing.T) {
		var n int
		var pw mock.PointsWriter
		pw.WritePointsFn = func(ctx context.Context, p []models.Point) error {
			orgID, bucketID := tsdb.DecodeNameSlice(p[0].Name())
			switch n++; n {
			case 1:
				if got, want := orgID, influxdb.ID(1); got != want {
					t.Fatalf("orgID=%d, want %d", got, want)
				} else if got, want := bucketID, influxdb.ID(2); got != want { // original bucket
					t.Fatalf("orgID=%d, want %d", got, want)
				}
				return errors.New("marker")
			case 2:
				if got, want := orgID, influxdb.ID(1); got != want {
					t.Fatalf("orgID=%d, want %d", got, want)
				} else if got, want := bucketID, influxdb.ID(10); got != want { // log bucket
					t.Fatalf("orgID=%d, want %d", got, want)
				}
				return nil
			default:
				t.Fatal("too many calls to WritePoints()")
				return nil
			}
		}

		var bs mock.BucketService
		bs.FindBucketsFn = func(ctx context.Context, filter influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
			if got, want := *filter.OrganizationID, influxdb.ID(1); got != want {
				t.Fatalf("orgID=%d, want %d", got, want)
			} else if got, want := *filter.Name, "logbkt"; got != want {
				t.Fatalf("name=%q, want %q", got, want)
			}
			return []*influxdb.Bucket{{ID: 10}}, 1, nil
		}

		lpw := &storage.LoggingPointsWriter{
			Underlying:    &pw,
			BucketFinder:  &bs,
			LogBucketName: "logbkt",
		}

		if err := lpw.WritePoints(context.Background(), []models.Point{models.MustNewPoint(
			tsdb.EncodeNameString(1, 2),
			models.NewTags(map[string]string{"t": "v"}),
			models.Fields{"f": float64(100)},
			time.Now(),
		)}); err == nil || err.Error() != `marker` {
			t.Fatalf("unexpected error: %#v", err)
		}

		// Expect two writes--the original and the logged.
		if got, want := n, 2; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}
	})

	// Ensure an error is returned if logging bucket cannot be found.
	t.Run("BucketError", func(t *testing.T) {
		var bs mock.BucketService
		bs.FindBucketsFn = func(ctx context.Context, filter influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
			return nil, 0, errors.New("bucket error")
		}

		lpw := &storage.LoggingPointsWriter{
			Underlying: &mock.PointsWriter{
				WritePointsFn: func(ctx context.Context, p []models.Point) error {
					return errors.New("point error")
				},
			},
			BucketFinder:  &bs,
			LogBucketName: "logbkt",
		}

		if err := lpw.WritePoints(context.Background(), []models.Point{models.MustNewPoint(
			tsdb.EncodeNameString(1, 2),
			models.NewTags(map[string]string{"t": "v"}),
			models.Fields{"f": float64(100)},
			time.Now(),
		)}); err == nil || err.Error() != `bucket error` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

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
