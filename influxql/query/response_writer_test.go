package query

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonFormatter_WriteResponse_Success(t *testing.T) {
	resp := Response{
		Results: []*Result{{
			StatementID: 0,
			Series: models.Rows{
				&models.Row{
					Name:    "cpu",
					Columns: []string{"time", "value"},
					Values:  [][]interface{}{{"2021-01-01T00:00:00Z", 42.0}},
				},
			},
		}},
	}

	for _, pretty := range []bool{false, true} {
		name := "compact"
		if pretty {
			name = "pretty"
		}
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			f := &jsonFormatter{Pretty: pretty}
			err := f.WriteResponse(context.Background(), &buf, resp)
			require.NoError(t, err)

			output := buf.Bytes()
			var parsed map[string]interface{}
			require.NoError(t, json.Unmarshal(output, &parsed))

			results, ok := parsed["results"].([]interface{})
			require.True(t, ok, "expected results array, got: %s", output)
			require.NotEmpty(t, results)

			result0, ok := results[0].(map[string]interface{})
			require.True(t, ok, "expected result object, got: %s", output)
			assert.Nil(t, result0["error"], "unexpected error in result")

			series, ok := result0["series"].([]interface{})
			require.True(t, ok, "expected series array, got: %s", output)
			require.NotEmpty(t, series)

			if pretty {
				assert.Contains(t, string(output), "    ", "expected indented output for pretty mode")
			}
		})
	}
}

func TestJsonFormatter_WriteResponse_MarshalError(t *testing.T) {
	tests := []struct {
		name    string
		value   interface{}
		wantErr string
	}{
		{"Inf", math.Inf(1), "unsupported value"},
		{"NaN", math.NaN(), "unsupported value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := Response{
				Results: []*Result{{
					StatementID: 0,
					Series: models.Rows{
						&models.Row{
							Name:    "cpu",
							Columns: []string{"time", "value"},
							Values:  [][]interface{}{{"2021-01-01T00:00:00Z", tt.value}},
						},
					},
				}},
			}

			var buf bytes.Buffer
			f := &jsonFormatter{Pretty: false}
			err := f.WriteResponse(context.Background(), &buf, resp)
			require.NoError(t, err)

			output := buf.Bytes()
			var parsed map[string]interface{}
			require.NoError(t, json.Unmarshal(output, &parsed), "output is not valid JSON: %s", output)

			results, ok := parsed["results"].([]interface{})
			require.True(t, ok, "expected results array, got: %s", output)
			require.NotEmpty(t, results)

			result0, ok := results[0].(map[string]interface{})
			require.True(t, ok, "expected result object, got: %s", output)

			errMsg, ok := result0["error"].(string)
			require.True(t, ok, "expected error string in result, got: %s", output)
			require.Contains(t, errMsg, tt.wantErr)
		})
	}
}
