package engine

import "github.com/influxdb/influxdb/protocol"

type MergeEngine struct {
	name string
	next Processor
}

func NewMergeEngine(table1, table2 string, ascending bool, next Processor) Processor {
	name := table1 + "_merge_" + table2

	me := &MergeEngine{name: name, next: next}

	return NewCommonMergeEngine(table1, table2, true, ascending, me)
}

func (me *MergeEngine) Yield(s *protocol.Series) (bool, error) {
	oldName := s.Name
	s.Name = &me.name
	s.Fields = append(s.Fields, "_orig_series")
	for _, p := range s.Points {
		p.Values = append(p.Values, &protocol.FieldValue{StringValue: oldName})
	}
	return me.next.Yield(s)
}

func (me *MergeEngine) Close() error {
	return me.next.Close()
}

func (me *MergeEngine) Name() string {
	return "MergeEngine"
}
