package influx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/id"
)

const (
	// AllAnnotations returns all annotations from the chronograf database
	AllAnnotations = `SELECT "start_time", "modified_time_ns", "text", "type", "id" FROM "annotations" WHERE "deleted"=false AND time >= %dns and "start_time" <= %d ORDER BY time DESC`
	// GetAnnotationID returns all annotations from the chronograf database where id is %s
	GetAnnotationID = `SELECT "start_time", "modified_time_ns", "text", "type", "id" FROM "annotations" WHERE "id"='%s' AND "deleted"=false ORDER BY time DESC`
	// AnnotationsDB is chronograf.  Perhaps later we allow this to be changed
	AnnotationsDB = "chronograf"
	// DefaultRP is autogen. Perhaps later we allow this to be changed
	DefaultRP = "autogen"
	// DefaultMeasurement is annotations.
	DefaultMeasurement = "annotations"
)

var _ chronograf.AnnotationStore = &AnnotationStore{}

// AnnotationStore stores annotations within InfluxDB
type AnnotationStore struct {
	client chronograf.TimeSeries
	id     chronograf.ID
	now    Now
}

// NewAnnotationStore constructs an annoation store with a client
func NewAnnotationStore(client chronograf.TimeSeries) *AnnotationStore {
	return &AnnotationStore{
		client: client,
		id:     &id.UUID{},
		now:    time.Now,
	}
}

// All lists all Annotations
func (a *AnnotationStore) All(ctx context.Context, start, stop time.Time) ([]chronograf.Annotation, error) {
	return a.queryAnnotations(ctx, fmt.Sprintf(AllAnnotations, start.UnixNano(), stop.UnixNano()))
}

// Get retrieves an annotation
func (a *AnnotationStore) Get(ctx context.Context, id string) (*chronograf.Annotation, error) {
	annos, err := a.queryAnnotations(ctx, fmt.Sprintf(GetAnnotationID, id))
	if err != nil {
		return nil, err
	}
	if len(annos) == 0 {
		return nil, chronograf.ErrAnnotationNotFound
	}
	return &annos[0], nil
}

// Add creates a new annotation in the store
func (a *AnnotationStore) Add(ctx context.Context, anno *chronograf.Annotation) (*chronograf.Annotation, error) {
	var err error
	anno.ID, err = a.id.Generate()
	if err != nil {
		return nil, err
	}
	return anno, a.client.Write(ctx, []chronograf.Point{
		toPoint(anno, a.now()),
	})
}

// Delete removes the annotation from the store
func (a *AnnotationStore) Delete(ctx context.Context, id string) error {
	cur, err := a.Get(ctx, id)
	if err != nil {
		return err
	}
	return a.client.Write(ctx, []chronograf.Point{
		toDeletedPoint(cur, a.now()),
	})
}

// Update replaces annotation; if the annotation's time is different, it
// also removes the previous annotation
func (a *AnnotationStore) Update(ctx context.Context, anno *chronograf.Annotation) error {
	cur, err := a.Get(ctx, anno.ID)
	if err != nil {
		return err
	}

	if err := a.client.Write(ctx, []chronograf.Point{toPoint(anno, a.now())}); err != nil {
		return err
	}

	// If the updated annotation has a different time, then, we must
	// delete the previous annotation
	if !cur.EndTime.Equal(anno.EndTime) {
		return a.client.Write(ctx, []chronograf.Point{
			toDeletedPoint(cur, a.now()),
		})
	}
	return nil
}

// queryAnnotations queries the chronograf db and produces all annotations
func (a *AnnotationStore) queryAnnotations(ctx context.Context, query string) ([]chronograf.Annotation, error) {
	res, err := a.client.Query(ctx, chronograf.Query{
		Command: query,
		DB:      AnnotationsDB,
		Epoch:   "ns",
	})
	if err != nil {
		return nil, err
	}
	octets, err := res.MarshalJSON()
	if err != nil {
		return nil, err
	}

	results := influxResults{}
	d := json.NewDecoder(bytes.NewReader(octets))
	d.UseNumber()
	if err := d.Decode(&results); err != nil {
		return nil, err
	}
	return results.Annotations()
}

func toPoint(anno *chronograf.Annotation, now time.Time) chronograf.Point {
	return chronograf.Point{
		Database:        AnnotationsDB,
		RetentionPolicy: DefaultRP,
		Measurement:     DefaultMeasurement,
		Time:            anno.EndTime.UnixNano(),
		Tags: map[string]string{
			"id": anno.ID,
		},
		Fields: map[string]interface{}{
			"deleted":          false,
			"start_time":       anno.StartTime.UnixNano(),
			"modified_time_ns": int64(now.UnixNano()),
			"text":             anno.Text,
			"type":             anno.Type,
		},
	}
}

func toDeletedPoint(anno *chronograf.Annotation, now time.Time) chronograf.Point {
	return chronograf.Point{
		Database:        AnnotationsDB,
		RetentionPolicy: DefaultRP,
		Measurement:     DefaultMeasurement,
		Time:            anno.EndTime.UnixNano(),
		Tags: map[string]string{
			"id": anno.ID,
		},
		Fields: map[string]interface{}{
			"deleted":          true,
			"start_time":       int64(0),
			"modified_time_ns": int64(now.UnixNano()),
			"text":             "",
			"type":             "",
		},
	}
}

type value []interface{}

func (v value) Int64(idx int) (int64, error) {
	if idx >= len(v) {
		return 0, fmt.Errorf("index %d does not exist in values", idx)
	}
	n, ok := v[idx].(json.Number)
	if !ok {
		return 0, fmt.Errorf("value at index %d is not int64, but, %T", idx, v[idx])
	}
	return n.Int64()
}

func (v value) Time(idx int) (time.Time, error) {
	tm, err := v.Int64(idx)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, tm), nil
}

func (v value) String(idx int) (string, error) {
	if idx >= len(v) {
		return "", fmt.Errorf("index %d does not exist in values", idx)
	}
	str, ok := v[idx].(string)
	if !ok {
		return "", fmt.Errorf("value at index %d is not string, but, %T", idx, v[idx])
	}
	return str, nil
}

type influxResults []struct {
	Series []struct {
		Values []value `json:"values"`
	} `json:"series"`
}

// annotationResult is an intermediate struct to track the latest modified
// time of an annotation
type annotationResult struct {
	chronograf.Annotation
	// modTime is bookkeeping to handle the case when an update fails; the latest
	// modTime will be the record returned
	modTime int64
}

// Annotations converts AllAnnotations query to annotations
func (r *influxResults) Annotations() (res []chronograf.Annotation, err error) {
	annos := map[string]annotationResult{}
	for _, u := range *r {
		for _, s := range u.Series {
			for _, v := range s.Values {
				anno := annotationResult{}

				if anno.EndTime, err = v.Time(0); err != nil {
					return
				}

				if anno.StartTime, err = v.Time(1); err != nil {
					return
				}

				if anno.modTime, err = v.Int64(2); err != nil {
					return
				}

				if anno.Text, err = v.String(3); err != nil {
					return
				}

				if anno.Type, err = v.String(4); err != nil {
					return
				}

				if anno.ID, err = v.String(5); err != nil {
					return
				}

				// If there are two annotations with the same id, take
				// the annotation with the latest modification time
				// This is to prevent issues when an update or delete fails.
				// Updates and deletes are multiple step queries.
				prev, ok := annos[anno.ID]
				if !ok || anno.modTime > prev.modTime {
					annos[anno.ID] = anno
				}
			}
		}
	}
	res = []chronograf.Annotation{}
	for _, a := range annos {
		res = append(res, a.Annotation)
	}

	sort.Slice(res, func(i int, j int) bool {
		return res[i].StartTime.Before(res[j].StartTime) || res[i].ID < res[j].ID
	})

	return res, err
}
