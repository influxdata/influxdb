package influxql

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/platform"
)

// MultiResultEncoder encodes results as InfluxQL JSON format.
type MultiResultEncoder struct{}

// Encode writes a collection of results to the influxdb 1.X http response format.
// Expectations/Assumptions:
//  1.  Each result will be published as a 'statement' in the top-level list of results.  The 'staementID'
//      will be interpreted as an integer, and will return an error otherwise.
//  2.  If the _field name is present in the tags, and a _value column is present, the _value column will
//      be renamed to the value of the _field tag
//  3.  If the _measurement name is present in the tags, it will be used as the row.Name for all rows.
//      Otherwise, we'll use the column value, which _must_ be present in that case.

func (e *MultiResultEncoder) Encode(w io.Writer, results platform.ResultIterator) error {
	resp := Response{}

	for results.More() {
		name, r := results.Next()

		blocks := r.Blocks()
		nameInt, err := strconv.Atoi(name)
		if err != nil {
			return fmt.Errorf("error converting result name to integer: %s", name)
		}
		result := Result{StatementID: nameInt}

		err = blocks.Do(func(b execute.Block) error {
			r := NewRow()

			fieldName := ""
			measurementVaries := -1
			for j, c := range b.Key().Cols() {
				if c.Type != execute.TString {
					return fmt.Errorf("partition column %q is not a string type", c.Label)
				}
				v := b.Key().Value(j).(string)
				if c.Label == "_measurement" {
					r.Name = v
				} else if c.Label == "_field" {
					fieldName = v
				} else {
					r.Tags[c.Label] = v
				}
			}

			for i, c := range b.Cols() {
				if c.Label == "_time" {
					r.Columns = append(r.Columns, "time")
				} else if c.Label == "_value" && fieldName != "" {
					r.Columns = append(r.Columns, fieldName)
				} else if !b.Key().HasCol(c.Label) {
					r.Columns = append(r.Columns, c.Label)
					if r.Name == "" && c.Label == "_measurement" {
						measurementVaries = i
					}
				}
			}
			if r.Name == "" && measurementVaries == -1 {
				return fmt.Errorf("no Measurement name found in result blocks for result: %s", name)
			}

			timeIdx := execute.ColIdx(execute.DefaultTimeColLabel, b.Cols())
			if timeIdx < 0 {
				return errors.New("table must have an _time column")
			}
			if typ := b.Cols()[timeIdx].Type; typ != execute.TTime {
				return fmt.Errorf("column _time must be of type Time got %v", typ)
			}
			err := b.Do(func(cr execute.ColReader) error {
				ts := cr.Times(timeIdx)
				for i := range ts {
					var v []interface{}

					for j, c := range cr.Cols() {
						if cr.Key().HasCol(c.Label) {
							continue
						}

						if j == measurementVaries {
							if c.Type != execute.TString {
								return errors.New("unexpected type, _measurement is not a string")
							}
							r.Name = cr.Strings(j)[i]
							continue
						}

						switch c.Type {
						case execute.TFloat:
							v = append(v, cr.Floats(j)[i])
						case execute.TInt:
							v = append(v, cr.Ints(j)[i])
						case execute.TString:
							v = append(v, cr.Strings(j)[i])
						case execute.TUInt:
							v = append(v, cr.UInts(j)[i])
						case execute.TBool:
							v = append(v, cr.Bools(j)[i])
						case execute.TTime:
							v = append(v, cr.Times(j)[i].Time().Format(time.RFC3339))
						default:
							v = append(v, "unknown")
						}
					}

					r.Values = append(r.Values, v)
				}
				return nil
			})
			if err != nil {
				return err
			}

			result.Series = append(result.Series, r)
			return nil
		})
		if err != nil {
			return fmt.Errorf("error iterating through results: %s", err)
		}
		resp.Results = append(resp.Results, result)
	}

	return json.NewEncoder(w).Encode(resp)
}
func NewMultiResultEncoder() *MultiResultEncoder {
	return new(MultiResultEncoder)
}
