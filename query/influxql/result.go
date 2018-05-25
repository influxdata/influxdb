package influxql

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/influxdata/platform/query"
)

// MultiResultEncoder encodes results as InfluxQL JSON format.
type MultiResultEncoder struct{}

// Encode writes a collection of results to the influxdb 1.X http response format.
// Expectations/Assumptions:
//  1.  Each result will be published as a 'statement' in the top-level list of results. The result name
//      will be interpreted as an integer and used as the statement id.
//  2.  If the _measurement name is present in the partition key, it will be used as the result name instead
//      of as a normal tag.
//  3.  All columns in the partition key must be strings and they will be used as tags. There is no current way
//      to have a tag and field be the same name in the results.
//      TODO(jsternberg): For full compatibility, the above must be possible.
//  4.  All other columns are fields and will be output in the order they are found.
//      TODO(jsternberg): This function currently requires the first column to be a time field, but this isn't
//      a strict requirement and will be lifted when we begin to work on transpiling meta queries.
func (e *MultiResultEncoder) Encode(w io.Writer, results query.ResultIterator) error {
	resp := Response{}

	for results.More() {
		r := results.Next()
		name := r.Name()
		id, err := strconv.Atoi(name)
		if err != nil {
			resp.error(fmt.Errorf("unable to parse statement id from result name: %s", err))
			results.Cancel()
			break
		}

		blocks := r.Blocks()

		result := Result{StatementID: id}
		if err := blocks.Do(func(b query.Block) error {
			var r Row

			for j, c := range b.Key().Cols() {
				if c.Type != query.TString {
					return fmt.Errorf("partition column %q is not a string type", c.Label)
				}
				v := b.Key().Value(j).Str()
				if c.Label == "_measurement" {
					r.Name = v
				} else {
					if r.Tags == nil {
						r.Tags = make(map[string]string)
					}
					r.Tags[c.Label] = v
				}
			}

			for _, c := range b.Cols() {
				if c.Label == "time" {
					r.Columns = append(r.Columns, "time")
				} else if !b.Key().HasCol(c.Label) {
					r.Columns = append(r.Columns, c.Label)
				}
			}

			if err := b.Do(func(cr query.ColReader) error {
				var values [][]interface{}
				j := 0
				for idx, c := range b.Cols() {
					if cr.Key().HasCol(c.Label) {
						continue
					}

					// Use the first column, usually time, to pre-generate all of the value containers.
					if j == 0 {
						switch c.Type {
						case query.TTime:
							values = make([][]interface{}, len(cr.Times(0)))
						default:
							// TODO(jsternberg): Support using other columns. This will
							// mostly be necessary for meta queries.
							return errors.New("first column must be time")
						}

						for j := range values {
							values[j] = make([]interface{}, len(r.Columns))
						}
					}

					// Fill in the values for each column.
					switch c.Type {
					case query.TFloat:
						for i, v := range cr.Floats(idx) {
							values[i][j] = v
						}
					case query.TInt:
						for i, v := range cr.Ints(idx) {
							values[i][j] = v
						}
					case query.TString:
						for i, v := range cr.Strings(idx) {
							values[i][j] = v
						}
					case query.TUInt:
						for i, v := range cr.UInts(idx) {
							values[i][j] = v
						}
					case query.TBool:
						for i, v := range cr.Bools(idx) {
							values[i][j] = v
						}
					case query.TTime:
						for i, v := range cr.Times(idx) {
							values[i][j] = v.Time().Format(time.RFC3339)
						}
					default:
						return fmt.Errorf("unsupported column type: %s", c.Type)
					}
					j++
				}
				r.Values = append(r.Values, values...)
				return nil
			}); err != nil {
				return err
			}

			result.Series = append(result.Series, &r)
			return nil
		}); err != nil {
			resp.error(err)
			results.Cancel()
			break
		}
		resp.Results = append(resp.Results, result)
	}

	if err := results.Err(); err != nil && resp.Err == "" {
		resp.error(err)
	}

	return json.NewEncoder(w).Encode(resp)
}
func NewMultiResultEncoder() *MultiResultEncoder {
	return new(MultiResultEncoder)
}
