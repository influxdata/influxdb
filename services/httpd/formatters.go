package httpd

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/models"
)

type jsonFormatter struct {
	Pretty bool
}

func (f *jsonFormatter) ContentType() string {
	return "application/json"
}

func (f *jsonFormatter) WriteResponse(w io.Writer, resp Response) (n int, err error) {
	var b []byte
	if f.Pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}

	if err != nil {
		n, err = io.WriteString(w, err.Error())
	} else {
		n, err = w.Write(b)
	}

	w.Write([]byte("\n"))
	n++
	return n, err
}

func (f *jsonFormatter) WriteError(w io.Writer, err error) {
	m := map[string]string{"error": err.Error()}

	var b []byte
	if f.Pretty {
		b, err = json.MarshalIndent(m, "", "    ")
	} else {
		b, err = json.Marshal(m)
	}

	if err != nil {
		io.WriteString(w, err.Error())
	} else {
		w.Write(b)
	}

	w.Write([]byte("\n"))
}

type csvFormatter struct {
	statementID int
	columns     []string
}

func (f *csvFormatter) ContentType() string {
	return "text/csv"
}

func (f *csvFormatter) WriteResponse(w io.Writer, resp Response) (n int, err error) {
	csv := csv.NewWriter(w)
	if resp.Err != nil {
		csv.Write([]string{"error"})
		csv.Write([]string{resp.Err.Error()})
		csv.Flush()
		return n, csv.Error()
	}

	for _, result := range resp.Results {
		if result.StatementID != f.statementID {
			// If there are no series in the result, skip past this result.
			if len(result.Series) == 0 {
				continue
			}

			// Set the statement id and print out a newline if this is not the first statement.
			if f.statementID >= 0 {
				// Flush the csv writer and write a newline.
				csv.Flush()
				if err := csv.Error(); err != nil {
					return n, err
				}

				out, err := io.WriteString(w, "\n")
				if err != nil {
					return n, err
				}
				n += out
			}
			f.statementID = result.StatementID

			// Print out the column headers from the first series.
			f.columns = make([]string, 2+len(result.Series[0].Columns))
			f.columns[0] = "name"
			f.columns[1] = "tags"
			copy(f.columns[2:], result.Series[0].Columns)
			if err := csv.Write(f.columns); err != nil {
				return n, err
			}
		}

		for _, row := range result.Series {
			f.columns[0] = row.Name
			if len(row.Tags) > 0 {
				f.columns[1] = string(models.NewTags(row.Tags).HashKey()[1:])
			} else {
				f.columns[1] = ""
			}
			for _, values := range row.Values {
				for i, value := range values {
					if value == nil {
						f.columns[i+2] = ""
						continue
					}

					switch v := value.(type) {
					case float64:
						f.columns[i+2] = strconv.FormatFloat(v, 'f', -1, 64)
					case int64:
						f.columns[i+2] = strconv.FormatInt(v, 10)
					case uint64:
						f.columns[i+2] = strconv.FormatUint(v, 10)
					case string:
						f.columns[i+2] = v
					case bool:
						if v {
							f.columns[i+2] = "true"
						} else {
							f.columns[i+2] = "false"
						}
					case time.Time:
						f.columns[i+2] = strconv.FormatInt(v.UnixNano(), 10)
					case *float64, *int64, *uint64, *string, *bool:
						f.columns[i+2] = ""
					}
				}
				csv.Write(f.columns)
			}
		}
	}
	csv.Flush()
	if err := csv.Error(); err != nil {
		return n, err
	}
	return n, nil
}

func (f *csvFormatter) WriteError(w io.Writer, err error) {
	csv := csv.NewWriter(w)
	csv.WriteAll([][]string{
		{"error"},
		{err.Error()},
	})
	csv.Flush()
}
