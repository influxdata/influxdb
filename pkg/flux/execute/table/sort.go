package table

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
)

// Sort will read a TableIterator and produce another TableIterator
// where the keys are sorted.
//
// This method will buffer all of the data since it needs to ensure
// all of the tables are read to avoid any deadlocks. Be careful
// using this method in performance sensitive areas.
func Sort(tables flux.TableIterator) (flux.TableIterator, error) {
	groups := execute.NewGroupLookup()
	if err := tables.Do(func(table flux.Table) error {
		buffered, err := execute.CopyTable(table)
		if err != nil {
			return err
		}
		groups.Set(buffered.Key(), buffered)
		return nil
	}); err != nil {
		return nil, err
	}

	var buffered []flux.Table
	groups.Range(func(_ flux.GroupKey, value interface{}) {
		buffered = append(buffered, value.(flux.Table))
	})
	return Iterator(buffered), nil
}
