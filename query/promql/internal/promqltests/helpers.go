package promqltests

import (
	"fmt"
	"sort"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	fpromql "github.com/influxdata/flux/promql"
	"github.com/influxdata/flux/semantic"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
)

func isErrorTable(tbl flux.Table) bool {
	cols := tbl.Cols()
	return len(cols) == 2 && cols[0].Label == "error" && cols[1].Label == "reference"
}

// FluxResultToPromQLValue translates a Flux result to a PromQL value
// of the desired type. For range query results, the passed-in value type
// should always be promql.ValueTypeMatrix (even if the root node for a
// range query can be of scalar type, range queries always return matrices).
func FluxResultToPromQLValue(result flux.Result, valType promql.ValueType) (promql.Value, error) {
	hashToSeries := map[uint64]*promql.Series{}

	err := result.Tables().Do(func(tbl flux.Table) error {
		if isErrorTable(tbl) {
			return fmt.Errorf("flux error: %s", tbl.Key().ValueString(0))
		}

		tbl.Do(func(cr flux.ColReader) error {
			// Each row corresponds to one PromQL metric / series.
			for i := 0; i < cr.Len(); i++ {
				builder := labels.NewBuilder(nil)
				var val float64
				var ts int64

				// Extract PromQL labels and timestamp/value from the columns.
				for j, col := range cr.Cols() {
					switch col.Label {
					case execute.DefaultTimeColLabel:
						ts = execute.ValueForRow(cr, i, j).Time().Time().UnixNano() / 1e6
					case execute.DefaultValueColLabel:
						v := execute.ValueForRow(cr, i, j)
						switch nat := v.Type().Nature(); nat {
						case semantic.Float:
							val = v.Float()
						default:
							return fmt.Errorf("invalid column value type: %s", nat.String())
						}
					case execute.DefaultStartColLabel, execute.DefaultStopColLabel, "_measurement":
						// Ignore.
						// Window boundaries are only interesting within the Flux pipeline.
						// _measurement is always set to the constant "prometheus" for now.
					default:
						ln := fpromql.UnescapeLabelName(col.Label)
						builder.Set(ln, cr.Strings(j).ValueString(i))
					}
				}

				lbls := builder.Labels()
				point := promql.Point{
					T: ts,
					V: val,
				}
				hash := lbls.Hash()
				if ser, ok := hashToSeries[hash]; !ok {
					hashToSeries[hash] = &promql.Series{
						Metric: lbls,
						Points: []promql.Point{point},
					}
				} else {
					ser.Points = append(ser.Points, point)
				}
			}
			return nil
		})
		return nil
	})

	if err != nil {
		return nil, err
	}

	switch valType {
	case promql.ValueTypeMatrix:
		matrix := make(promql.Matrix, 0, len(hashToSeries))
		for _, ser := range hashToSeries {
			matrix = append(matrix, *ser)
		}
		sort.Sort(matrix)
		return matrix, nil

	case promql.ValueTypeVector:
		vector := make(promql.Vector, 0, len(hashToSeries))
		for _, ser := range hashToSeries {
			if len(ser.Points) != 1 {
				return nil, fmt.Errorf("expected exactly one output point for every series for vector result")
			}
			vector = append(vector, promql.Sample{
				Metric: ser.Metric,
				Point:  ser.Points[0],
			})
		}
		// TODO: Implement sorting for vectors, but this is only needed for tests.
		// sort.Sort(vector)
		return vector, nil

	case promql.ValueTypeScalar:
		if len(hashToSeries) != 1 {
			return nil, fmt.Errorf("expected exactly one output series for scalar result")
		}
		for _, ser := range hashToSeries {
			if len(ser.Points) != 1 {
				return nil, fmt.Errorf("expected exactly one output point for scalar result")
			}
			return promql.Scalar{
				T: ser.Points[0].T,
				V: ser.Points[0].V,
			}, nil
		}
		// Should be unreachable due to the checks above.
		return nil, fmt.Errorf("no point found")
	default:
		return nil, fmt.Errorf("unsupported PromQL value type: %s", valType)
	}
}
