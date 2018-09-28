package tsdb

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/platform/models"
)

func TestSeriesCollection(t *testing.T) {
	// some helper functions. short names because local scope and frequently used.
	var (
		equal = reflect.DeepEqual
		b     = func(s string) []byte { return []byte(s) }
		bs    = func(s ...string) [][]byte {
			out := make([][]byte, len(s))
			for i := range s {
				out[i] = b(s[i])
			}
			return out
		}

		assertEqual = func(t *testing.T, name string, got, wanted interface{}) {
			t.Helper()
			if !equal(got, wanted) {
				t.Fatalf("bad %s: got: %v but wanted: %v", name, got, wanted)
			}
		}
	)

	t.Run("New", func(t *testing.T) {
		points := []models.Point{
			models.MustNewPoint("a", models.Tags{}, models.Fields{"f": 1.0}, time.Now()),
			models.MustNewPoint("b", models.Tags{}, models.Fields{"b": true}, time.Now()),
			models.MustNewPoint("c", models.Tags{}, models.Fields{"i": int64(1)}, time.Now()),
		}
		collection := NewSeriesCollection(points)

		assertEqual(t, "length", collection.Length(), 3)

		for iter := collection.Iterator(); iter.Next(); {
			ipt, spt := iter.Point(), points[iter.Index()]
			fi := spt.FieldIterator()
			fi.Next()

			assertEqual(t, "point", ipt, spt)
			assertEqual(t, "key", iter.Key(), spt.Key())
			assertEqual(t, "name", iter.Name(), spt.Name())
			assertEqual(t, "tags", iter.Tags(), spt.Tags())
			assertEqual(t, "type", iter.Type(), fi.Type())
		}
	})

	t.Run("Copy", func(t *testing.T) {
		collection := &SeriesCollection{
			Keys:  bs("ka", "kb", "kc"),
			Names: bs("na", "nb", "nc"),
		}

		collection.Copy(0, 2)
		assertEqual(t, "keys", collection.Keys, bs("kc", "kb", "kc"))
		assertEqual(t, "names", collection.Names, bs("nc", "nb", "nc"))

		collection.Copy(0, 4) // out of bounds
		assertEqual(t, "keys", collection.Keys, bs("kc", "kb", "kc"))
		assertEqual(t, "names", collection.Names, bs("nc", "nb", "nc"))
	})

	t.Run("Swap", func(t *testing.T) {
		collection := &SeriesCollection{
			Keys:  bs("ka", "kb", "kc"),
			Names: bs("na", "nb", "nc"),
		}

		collection.Swap(0, 2)
		assertEqual(t, "keys", collection.Keys, bs("kc", "kb", "ka"))
		assertEqual(t, "names", collection.Names, bs("nc", "nb", "na"))

		collection.Swap(0, 4) // out of bounds
		assertEqual(t, "keys", collection.Keys, bs("kc", "kb", "ka"))
		assertEqual(t, "names", collection.Names, bs("nc", "nb", "na"))
	})

	t.Run("Truncate", func(t *testing.T) {
		collection := &SeriesCollection{
			Keys:  bs("ka", "kb", "kc"),
			Names: bs("na", "nb", "nc"),
		}

		collection.Truncate(1)
		assertEqual(t, "keys", collection.Keys, bs("ka"))
		assertEqual(t, "names", collection.Names, bs("na"))

		collection.Truncate(0)
		assertEqual(t, "keys", collection.Keys, bs())
		assertEqual(t, "names", collection.Names, bs())
	})

	t.Run("Advance", func(t *testing.T) {
		collection := &SeriesCollection{
			Keys:  bs("ka", "kb", "kc"),
			Names: bs("na", "nb", "nc"),
		}

		collection.Advance(1)
		assertEqual(t, "keys", collection.Keys, bs("kb", "kc"))
		assertEqual(t, "names", collection.Names, bs("nb", "nc"))

		collection.Advance(1)
		assertEqual(t, "keys", collection.Keys, bs("kc"))
		assertEqual(t, "names", collection.Names, bs("nc"))
	})

	t.Run("InvalidateAll", func(t *testing.T) {
		collection := &SeriesCollection{Keys: bs("ka", "kb", "kc")}

		collection.InvalidateAll("test reason")
		assertEqual(t, "length", collection.Length(), 0)
		assertEqual(t, "error", collection.PartialWriteError(), PartialWriteError{
			Reason:      "test reason",
			Dropped:     3,
			DroppedKeys: bs("ka", "kb", "kc"),
		})
	})

	t.Run("Invalid", func(t *testing.T) {
		collection := &SeriesCollection{Keys: bs("ka", "kb", "kc")}

		// invalidate half the entries
		for iter := collection.Iterator(); iter.Next(); {
			if iter.Index()%2 == 0 {
				iter.Invalid("test reason")
			}
		}

		// nothing happens yet: all values are staged
		assertEqual(t, "length", collection.Length(), 3)

		// apply all of the invalid calls
		collection.ApplyConcurrentDrops()
		assertEqual(t, "length", collection.Length(), 1)
		assertEqual(t, "error", collection.PartialWriteError(), PartialWriteError{
			Reason:      "test reason",
			Dropped:     2,
			DroppedKeys: bs("ka", "kc"),
		})
	})
}
