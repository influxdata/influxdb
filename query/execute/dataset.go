package execute

import (
	"github.com/influxdata/ifql/query"
	uuid "github.com/satori/go.uuid"
)

// Dataset represents the set of data produced by a transformation.
type Dataset interface {
	Node

	RetractBlock(key PartitionKey) error
	UpdateProcessingTime(t Time) error
	UpdateWatermark(mark Time) error
	Finish(error)

	SetTriggerSpec(t query.TriggerSpec)
}

// DataCache holds all working data for a transformation.
type DataCache interface {
	Block(PartitionKey) (Block, error)

	ForEach(func(PartitionKey))
	ForEachWithContext(func(PartitionKey, Trigger, BlockContext))

	DiscardBlock(PartitionKey)
	ExpireBlock(PartitionKey)

	SetTriggerSpec(t query.TriggerSpec)
}

type AccumulationMode int

const (
	DiscardingMode AccumulationMode = iota
	AccumulatingMode
	AccumulatingRetractingMode
)

type DatasetID uuid.UUID

func (id DatasetID) String() string {
	return uuid.UUID(id).String()
}

var ZeroDatasetID DatasetID

func (id DatasetID) IsZero() bool {
	return id == ZeroDatasetID
}

type dataset struct {
	id DatasetID

	ts      []Transformation
	accMode AccumulationMode

	watermark      Time
	processingTime Time

	cache DataCache
}

func NewDataset(id DatasetID, accMode AccumulationMode, cache DataCache) *dataset {
	return &dataset{
		id:      id,
		accMode: accMode,
		cache:   cache,
	}
}

func (d *dataset) AddTransformation(t Transformation) {
	d.ts = append(d.ts, t)
}

func (d *dataset) SetTriggerSpec(spec query.TriggerSpec) {
	d.cache.SetTriggerSpec(spec)
}

func (d *dataset) UpdateWatermark(mark Time) error {
	d.watermark = mark
	if err := d.evalTriggers(); err != nil {
		return err
	}
	for _, t := range d.ts {
		if err := t.UpdateWatermark(d.id, mark); err != nil {
			return err
		}
	}
	return nil
}

func (d *dataset) UpdateProcessingTime(time Time) error {
	d.processingTime = time
	if err := d.evalTriggers(); err != nil {
		return err
	}
	for _, t := range d.ts {
		if err := t.UpdateProcessingTime(d.id, time); err != nil {
			return err
		}
	}
	return nil
}

func (d *dataset) evalTriggers() (err error) {
	d.cache.ForEachWithContext(func(key PartitionKey, trigger Trigger, bc BlockContext) {
		if err != nil {
			// Skip the rest once we have encountered an error
			return
		}
		c := TriggerContext{
			Block:                 bc,
			Watermark:             d.watermark,
			CurrentProcessingTime: d.processingTime,
		}

		if trigger.Triggered(c) {
			err = d.triggerBlock(key)
		}
		if trigger.Finished() {
			d.expireBlock(key)
		}
	})
	return err
}

func (d *dataset) triggerBlock(key PartitionKey) error {
	b, err := d.cache.Block(key)
	if err != nil {
		return err
	}
	b.RefCount(len(d.ts))
	switch d.accMode {
	case DiscardingMode:
		for _, t := range d.ts {
			if err := t.Process(d.id, b); err != nil {
				return err
			}
		}
		d.cache.DiscardBlock(key)
	case AccumulatingRetractingMode:
		for _, t := range d.ts {
			if err := t.RetractBlock(d.id, b.Key()); err != nil {
				return err
			}
		}
		fallthrough
	case AccumulatingMode:
		for _, t := range d.ts {
			if err := t.Process(d.id, b); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *dataset) expireBlock(key PartitionKey) {
	d.cache.ExpireBlock(key)
}

func (d *dataset) RetractBlock(key PartitionKey) error {
	d.cache.DiscardBlock(key)
	for _, t := range d.ts {
		if err := t.RetractBlock(d.id, key); err != nil {
			return err
		}
	}
	return nil
}

func (d *dataset) Finish(err error) {
	if err == nil {
		// Only trigger blocks we if we not finishing because of an error.
		d.cache.ForEach(func(bk PartitionKey) {
			if err != nil {
				return
			}
			err = d.triggerBlock(bk)
			d.cache.ExpireBlock(bk)
		})
	}
	for _, t := range d.ts {
		t.Finish(d.id, err)
	}
}
