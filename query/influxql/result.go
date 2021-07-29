package influxql

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/iocounter"
)

// MultiResultEncoder encodes results as InfluxQL JSON format.
type MultiResultEncoder struct{}

// Encode writes a collection of results to the influxdb 1.X http response format.
// Expectations/Assumptions:
//  1.  Each result will be published as a 'statement' in the top-level list of results. The result name
//      will be interpreted as an integer and used as the statement id.
//  2.  If the _measurement name is present in the group key, it will be used as the result name instead
//      of as a normal tag.
//  3.  All columns in the group key must be strings and they will be used as tags. There is no current way
//      to have a tag and field be the same name in the results.
//      TODO(jsternberg): For full compatibility, the above must be possible.
//  4.  All other columns are fields and will be output in the order they are found.
//      TODO(jsternberg): This function currently requires the first column to be a time field, but this isn't
//      a strict requirement and will be lifted when we begin to work on transpiling meta queries.
func (e *MultiResultEncoder) Encode(w io.Writer, results flux.ResultIterator) (int64, error) {
	resp := Response{}
	wc := &iocounter.Writer{Writer: w}

	for results.More() {
		res := results.Next()
		name := res.Name()
		id, err := strconv.Atoi(name)
		if err != nil {
			resp.error(fmt.Errorf("unable to parse statement id from result name: %s", err))
			results.Release()
			break
		}

		tables := res.Tables()

		result := Result{StatementID: id}
		if err := tables.Do(func(tbl flux.Table) error {
			var row Row

			for j, c := range tbl.Key().Cols() {
				if c.Type != flux.TString {
					// Skip any columns that aren't strings. They are extra ones that
					// flux includes by default like the start and end times that we do not
					// care about.
					continue
				}
				v := tbl.Key().Value(j).Str()
				if c.Label == "_measurement" {
					row.Name = v
				} else if c.Label == "_field" {
					// If the field key was not removed by a previous operation, we explicitly
					// ignore it here when encoding the result back.
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
			// from the ordering given in the original flux.
			resultColMap := map[string]int{}
			j := 1
			for _, c := range tbl.Cols() {
				if c.Label == execute.DefaultTimeColLabel {
					resultColMap[c.Label] = 0
				} else if !tbl.Key().HasCol(c.Label) {
					resultColMap[c.Label] = j
					j++
				}
			}

			if _, ok := resultColMap[execute.DefaultTimeColLabel]; !ok {
				for k, v := range resultColMap {
					resultColMap[k] = v - 1
				}
			}

			row.Columns = make([]string, len(resultColMap))
			for k, v := range resultColMap {
				if k == execute.DefaultTimeColLabel {
					k = "time"
				}
				row.Columns[v] = k
			}

			if err := tbl.Do(func(cr flux.ColReader) error {
				// Preallocate the number of rows for the response to make this section
				// of code easier to read. Find a time column which should exist
				// in the output.
				values := make([][]interface{}, cr.Len())
				for j := range values {
					values[j] = make([]interface{}, len(row.Columns))
				}

				j := 0
				for idx, c := range tbl.Cols() {
					if cr.Key().HasCol(c.Label) {
						continue
					}

					j = resultColMap[c.Label]
					// Fill in the values for each column.
					switch c.Type {
					case flux.TFloat:
						vs := cr.Floats(idx)
						for i := 0; i < vs.Len(); i++ {
							if vs.IsValid(i) {
								values[i][j] = vs.Value(i)
							}
						}
					case flux.TInt:
						vs := cr.Ints(idx)
						for i := 0; i < vs.Len(); i++ {
							if vs.IsValid(i) {
								values[i][j] = vs.Value(i)
							}
						}
					case flux.TString:
						vs := cr.Strings(idx)
						for i := 0; i < vs.Len(); i++ {
							if vs.IsValid(i) {
								values[i][j] = vs.Value(i)
							}
						}
					case flux.TUInt:
						vs := cr.UInts(idx)
						for i := 0; i < vs.Len(); i++ {
							if vs.IsValid(i) {
								values[i][j] = vs.Value(i)
							}
						}
					case flux.TBool:
						vs := cr.Bools(idx)
						for i := 0; i < vs.Len(); i++ {
							if vs.IsValid(i) {
								values[i][j] = vs.Value(i)
							}
						}
					case flux.TTime:
						vs := cr.Times(idx)
						for i := 0; i < vs.Len(); i++ {
							if vs.IsValid(i) {
								values[i][j] = execute.Time(vs.Value(i)).Time().Format(time.RFC3339Nano)
							}
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
			results.Release()
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
