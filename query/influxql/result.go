package influxql

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/iocounter"
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
func (e *MultiResultEncoder) Encode(w io.Writer, results query.ResultIterator) (int64, error) {
	resp := Response{}
	wc := &iocounter.Writer{Writer: w}

	for results.More() {
		res := results.Next()
		name := res.Name()
		id, err := strconv.Atoi(name)
		if err != nil {
			resp.error(fmt.Errorf("unable to parse statement id from result name: %s", err))
			results.Cancel()
			break
		}

		blocks := res.Blocks()

		result := Result{StatementID: id}
		if err := blocks.Do(func(b query.Block) error {
			var row Row

			for j, c := range b.Key().Cols() {
				if c.Type != query.TString {
					return fmt.Errorf("partition column %q is not a string type", c.Label)
				}
				v := b.Key().Value(j).Str()
				if c.Label == "_measurement" {
					row.Name = v
				} else {
					if row.Tags == nil {
						row.Tags = make(map[string]string)
					}
					row.Tags[c.Label] = v
				}
			}

			// TODO: resultColMap should be constructed from query metadata once it is provided.
			// for now we know that an influxql query ALWAYS has time first, so we put this placeholder
			// here to catch this most obvious requirement.  Column orderings should be explicitly determined
			// from the ordering given in the original query.
			resultColMap := map[string]int{}
			j := 1
			for _, c := range b.Cols() {
				if c.Label == "time" {
					resultColMap[c.Label] = 0
				} else if !b.Key().HasCol(c.Label) {
					resultColMap[c.Label] = j
					j++
				}
			}

			row.Columns = make([]string, len(resultColMap))
			for k, v := range resultColMap {
				row.Columns[v] = k
			}

			if err := b.Do(func(cr query.ColReader) error {
				// Preallocate the number of rows for the response to make this section
				// of code easier to read. Find a time column which should exist
				// in the output.
				values := make([][]interface{}, cr.Len())
				for j := range values {
					values[j] = make([]interface{}, len(row.Columns))
				}

				j := 0
				for idx, c := range b.Cols() {
					if cr.Key().HasCol(c.Label) {
						continue
					}

					j = resultColMap[c.Label]
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

				}
				row.Values = append(row.Values, values...)
				return nil
			}); err != nil {
				return err
			}

			result.Series = append(result.Series, &row)
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

	err := json.NewEncoder(wc).Encode(resp)
	return wc.Count(), err
}
func NewMultiResultEncoder() *MultiResultEncoder {
	return new(MultiResultEncoder)
}
