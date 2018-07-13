package executetest

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	uuid "github.com/satori/go.uuid"
)

func RandomDatasetID() execute.DatasetID {
	return execute.DatasetID(uuid.NewV4())
}

type Dataset struct {
	ID                    execute.DatasetID
	Retractions           []query.GroupKey
	ProcessingTimeUpdates []execute.Time
	WatermarkUpdates      []execute.Time
	Finished              bool
	FinishedErr           error
}

func NewDataset(id execute.DatasetID) *Dataset {
	return &Dataset{
		ID: id,
	}
}

func (d *Dataset) AddTransformation(t execute.Transformation) {
	panic("not implemented")
}

func (d *Dataset) RetractTable(key query.GroupKey) error {
	d.Retractions = append(d.Retractions, key)
	return nil
}

func (d *Dataset) UpdateProcessingTime(t execute.Time) error {
	d.ProcessingTimeUpdates = append(d.ProcessingTimeUpdates, t)
	return nil
}

func (d *Dataset) UpdateWatermark(mark execute.Time) error {
	d.WatermarkUpdates = append(d.WatermarkUpdates, mark)
	return nil
}

func (d *Dataset) Finish(err error) {
	if d.Finished {
		panic("finish has already been called")
	}
	d.Finished = true
	d.FinishedErr = err
}

func (d *Dataset) SetTriggerSpec(t query.TriggerSpec) {
	panic("not implemented")
}

type NewTransformation func(execute.Dataset, execute.TableBuilderCache) execute.Transformation

func TransformationPassThroughTestHelper(t *testing.T, newTr NewTransformation) {
	t.Helper()

	now := execute.Now()
	d := NewDataset(RandomDatasetID())
	c := execute.NewTableBuilderCache(UnlimitedAllocator)
	c.SetTriggerSpec(execute.DefaultTriggerSpec)

	parentID := RandomDatasetID()
	tr := newTr(d, c)
	if err := tr.UpdateWatermark(parentID, now); err != nil {
		t.Fatal(err)
	}
	if err := tr.UpdateProcessingTime(parentID, now); err != nil {
		t.Fatal(err)
	}
	tr.Finish(parentID, nil)

	exp := &Dataset{
		ID: d.ID,
		ProcessingTimeUpdates: []execute.Time{now},
		WatermarkUpdates:      []execute.Time{now},
		Finished:              true,
		FinishedErr:           nil,
	}
	if !cmp.Equal(d, exp) {
		t.Errorf("unexpected dataset -want/+got\n%s", cmp.Diff(exp, d))
	}
}
