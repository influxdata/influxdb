package engine

import (
	"protocol"
)

func getMergeYield(table1, table2 string, yield func(*protocol.Series) error) func(*protocol.Series) error {
	s1 := []*protocol.Series{}
	s2 := []*protocol.Series{}
	done1 := false
	done2 := false
	mergedSeriesName := table1 + "_merge_" + table2
	var nullValues1 []*protocol.FieldValue
	var nullValues2 []*protocol.FieldValue
	var fields1 []string
	var fields2 []string

	return func(p *protocol.Series) error {
		current := &s1
		other := &s2
		currentDone := &done1
		otherDone := &done2
		currentNullValues := &nullValues1
		otherNullValues := &nullValues2
		currentFields := &fields1
		otherFields := &fields2

		if *p.Name == table2 {
			current, other = other, current
			currentDone, otherDone = otherDone, currentDone
			currentNullValues, otherNullValues = otherNullValues, currentNullValues
			currentFields, otherFields = otherFields, currentFields
		}

		// setup the fields
		if *currentFields == nil {
			for _, f := range p.Fields {
				*currentFields = append(*currentFields, *p.Name+"."+f)
			}
			for i := 0; i < len(p.Fields); i++ {
				*currentNullValues = append(*currentNullValues, nil)
			}
		}

		// data for current table is exhausted
		if len(p.Points) == 0 {
			*currentDone = true
		} else {
			*current = append(*current, p)
		}

		// if data for the other table is exhausted then pass through
		if *otherDone && len(*other) == 0 {
			for _, s := range *current {
				for _, p := range s.Points {
					p.Values = append(p.Values, *otherNullValues...)
				}

				err := yield(&protocol.Series{
					Name:   &mergedSeriesName,
					Fields: append(*currentFields, *otherFields...),
					Points: s.Points,
				})
				if err != nil {
					return err
				}
			}
			*current = nil
		}

		// if data for the current table is exhausted then pass through
		if *currentDone && len(*current) == 0 {
			for _, s := range *other {
				for _, p := range s.Points {
					p.Values = append(p.Values, *currentNullValues...)
				}

				err := yield(&protocol.Series{
					Name:   &mergedSeriesName,
					Fields: append(*otherFields, *currentFields...),
					Points: s.Points,
				})
				if err != nil {
					return err
				}
			}
			*other = nil
		}

		for len(s1) > 0 && len(s2) > 0 && len(s1[0].Points) > 0 && len(s2[0].Points) > 0 {
			var points []*protocol.Point

			if *s1[0].Points[0].Timestamp > *s2[0].Points[0].Timestamp {
				// s1 fields = null, s2 fields = some values
				points = s2[0].Points[:1]
				for _, p := range points {
					p.Values = append(nullValues1, p.Values...)
				}
				s2[0].Points = s2[0].Points[1:]
				if len(s2[0].Points) == 0 {
					s2 = s2[1:]
				}
			} else {
				// s1 fields = null, s2 fields = some values
				points = s1[0].Points[:1]
				for _, p := range points {
					p.Values = append(p.Values, nullValues2...)
				}
				s1[0].Points = s1[0].Points[1:]
				if len(s1[0].Points) == 0 {
					s1 = s1[1:]
				}
			}
			err := yield(&protocol.Series{
				Name:   &mergedSeriesName,
				Fields: append(fields1, fields2...),
				Points: points,
			})
			if err != nil {
				return err
			}
		}
		return nil
	}
}
