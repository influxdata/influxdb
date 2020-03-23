package write

import (
	"bytes"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func readCsv(t *testing.T, data string) [][]string {
	reader := csv.NewReader(strings.NewReader(data))
	var rows [][]string
	for {
		row, err := reader.Read()
		reader.FieldsPerRecord = 0 // every row can have different number of fields
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Log("row: ", row)
			t.Log(err)
			t.Fail()
		}
		rows = append(rows, row)
	}
	return rows
}

// TestQueryResult validates construction of table columns from Query CSV result
func TestQueryResult(t *testing.T) {
	const csvQueryResult = `
#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:17:57Z,0,time_steal,cpu,cpu1,rsavage.prod
,,0,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:18:07Z,0,time_steal,cpu,cpu1,rsavage.prod

#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,1,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:18:01Z,2.7263631815907954,usage_user,cpu,cpu-total,tahoecity.prod
,,1,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:18:11Z,2.247752247752248,usage_user,cpu,cpu-total,tahoecity.prod
#unre`

	var lineProtocolQueryResult = []string{
		"cpu,cpu=cpu1,host=rsavage.prod time_steal=0 1582669077000000000",
		"cpu,cpu=cpu1,host=rsavage.prod time_steal=0 1582669087000000000",
		"cpu,cpu=cpu-total,host=tahoecity.prod usage_user=2.7263631815907954 1582669081000000000",
		"cpu,cpu=cpu-total,host=tahoecity.prod usage_user=2.247752247752248 1582669091000000000",
	}

	table := CsvTable{}
	rows := readCsv(t, csvQueryResult)
	lineProtocolIndex := 0
	for i, row := range rows {
		rowProcessed := table.AddRow(row)
		if i%6 < 4 {
			require.Equal(t, rowProcessed, false, "row %d", i)
		} else {
			require.Equal(t, rowProcessed, true, "row %d", i)
			line, _ := table.CreateLine(row)
			require.Equal(t, lineProtocolQueryResult[lineProtocolIndex], line)
			lineProtocolIndex++
			if i%6 == 4 {
				// verify table
				require.GreaterOrEqual(t, len(table.columns), 10)
				require.Equal(t, table.columns, table.Columns())
				for j, col := range table.columns {
					if j > 0 {
						require.Equal(t, col.Index, j)
						require.Equal(t, col.Label, rows[i-1][j])
						if len(rows[i-2]) > j {
							require.Equal(t, col.DefaultValue, rows[i-2][j])
						} else {
							// some traling data are missing
							require.Equal(t, col.DefaultValue, "")
						}
						require.Equal(t, col.DataType, rows[i-3][j])
						require.Equal(t, col.LinePart == linePartTag, rows[i-4][j] == "true")
					}
				}
				// verify cached values
				table.computeIndexes()
				require.Equal(t, table.Column("_measurement"), table.cachedMeasurement)
				require.Nil(t, table.Column("_no"))
				require.NotNil(t, table.cachedMeasurement)
				require.NotNil(t, table.cachedFieldName)
				require.NotNil(t, table.cachedFieldValue)
				require.NotNil(t, table.cachedTime)
				require.NotNil(t, table.cachedTags)
				require.Equal(t, table.Measurement().Label, "_measurement")
				require.Equal(t, table.FieldName().Label, "_field")
				require.Equal(t, table.FieldValue().Label, "_value")
				require.Equal(t, table.Time().Label, "_time")
				require.Equal(t, len(table.Tags()), 2)
				require.Equal(t, table.Tags()[0].Label, "cpu")
				require.Equal(t, table.Tags()[1].Label, "host")
				require.Equal(t, len(table.Fields()), 0)
			}
		}
	}
}

//Test_ignoreLeadingComment
func Test_ignoreLeadingComment(t *testing.T) {
	var tests = []struct {
		value  string
		expect string
	}{
		{"", ""},
		{"a", "a"},
		{" #whatever", " #whatever"},
		{"#whatever", ""},
		{"#whatever ", ""},
		{"#whatever a b ", "a b "},
		{"#whatever  a b ", "a b "},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			require.Equal(t, test.expect, ignoreLeadingComment(test.value))
		})
	}

}

// TestCsvData checks data that are writen in an annotated CSV file
func TestCsvData(t *testing.T) {
	var tests = []struct {
		name string
		csv  string
		line string
	}{
		{
			"simple1",
			"_measurement,a,b\ncpu,1,1",
			"cpu a=1,b=1",
		},
		{
			"simple1b",
			"_measurement,,a,b\ncpu,whatever,1,1",
			"cpu a=1,b=1",
		},
		{
			"simple2",
			"_measurement\ncpu,1,1",
			"", // no fields present
		},
		{
			"simple3",
			"_time\n1,1",
			"", // no measurement present
		},
		{
			"annotated1",
			"#linepart measurement,,\nmeasurement,a,b\ncpu,1,2",
			"cpu a=1,b=2",
		},
		{
			"annotated2",
			"#linepart measurement,tag,field\nmeasurement,a,b\ncpu,1,2",
			"cpu,a=1 b=2",
		},
		{
			"annotated3",
			"#linepart measurement,tag,time,field\nmeasurement,a,b,time\ncpu,1,2,3",
			"cpu,a=1 time=3 2",
		},
		{
			"annotated3_detectedTime1",
			"#linepart measurement,tag,time,field\nmeasurement,a,b,time\ncpu,1,2020-01-10T10:10:10Z,3",
			"cpu,a=1 time=3 1578651010000000000",
		},
		{
			"annotated3_detectedTime2",
			"#linepart measurement,tag,time,field\nmeasurement,a,b,time\ncpu,1,2020-01-10T10:10:10.0Z,3",
			"cpu,a=1 time=3 1578651010000000000",
		},
		{
			"annotated4",
			"#linepart measurement,tag,ignore,field\nmeasurement,a,b,time\ncpu,1,2,3",
			"cpu,a=1 time=3",
		},
		{
			"annotated5",
			"#linepart measurement,tag,ignore,field\nmeasurement,a,b,time\ncpu,1,2,3",
			"cpu,a=1 time=3",
		},
		{
			"annotated6",
			"#linepart measurement,tag,ignore,field\n" +
				"#lineparta tag,tag,\n" + // this must be ignored since it not a control comment
				"measurement,a,b,time\ncpu,1,2,3",
			"cpu,a=1 time=3",
		},
		{
			"annotated7",
			"#linepart measurement,,\n#datatype ,dateTime:RFC3339Nano,\nmeasurement,a,b\ncpu,2020-01-10T10:10:10.0Z,2",
			"cpu a=1578651010000000000,b=2",
		},
		{
			"annotated8",
			"#linepart measurement,,,field\nmeasurement,_field,_value,other\ncpu,a,1,2",
			"cpu a=1,other=2",
		},
		{
			"annotated9_sortedTags",
			"#linepart measurement,tag,tag,time,field\nmeasurement,b,a,c,time\ncpu,1,2,3,4",
			"cpu,a=2,b=1 time=4 3",
		},
		{
			"allTypes1",
			"#datatype ,string,double,boolean,long,unsignedLong,duration,base64Binary,dateTime:RFC3339,dateTime:RFC3339Nano,\n" +
				"_measurement,s,d,b,l,ul,dur,by,d1,d2,_time\n" +
				`cpu,"str",1.0,true,1,1,1ms,YWFh,2020-01-10T10:10:10Z,2020-01-10T10:10:10Z,1`,
			"cpu s=\"str\",d=1,b=true,l=1i,ul=1u,dur=1000000i,by=YWFh,d1=1578651010000000000,d2=1578651010000000000 1",
		},
		{
			"allTypes_escaped",
			"#datatype ,string,string,,,,\n" +
				`_measurement,s1,s2,"a,","b ",c=` + "\n" +
				`"cpu, ","""",\,a,b,c`,
			`cpu\,\  s1="\"",s2="\\",a\,=a,b\ =b,c\==c`,
		},
		{
			"default_values",
			"#default cpu,0,1\n_measurement,col1,_time\n,,",
			"cpu col1=0 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := readCsv(t, test.csv)
			table := CsvTable{}
			var lines []string
			for _, row := range rows {
				rowProcessed := table.AddRow(row)
				if rowProcessed {
					line, err := table.CreateLine(row)
					if err != nil && test.line != "" {
						require.Nil(t, err.Error())
					}
					lines = append(lines, line)
				}
			}
			require.Equal(t, []string{test.line}, lines)
		})
	}
}

// TestCsvData_dataErrors validates table data errors
func TestCsvData_dataErrors(t *testing.T) {
	var tests = []struct {
		name string
		csv  string
	}{
		{
			"error_1_is_not_dateTime:RFC3339",
			"#linepart measurement,,\n#datatype ,dateTime:RFC3339,\nmeasurement,a,b\ncpu,1,2",
		},
		{
			"error_a_fieldValue_is_not_long",
			"#linepart measurement,,\n#datatype ,long,\nmeasurement,_value,_field\ncpu,a,count",
		},
		{
			"error_a_is_not_long",
			"#linepart measurement,,\n#datatype ,long,\nmeasurement,a,b\ncpu,a,2",
		},
		{
			"error_time_is_not_time",
			"#linepart measurement,tag,time,field\nmeasurement,a,b,time\ncpu,1,2020-10,3",
		},
		{
			"error_no_measurement",
			"#datatype ,\ncol1,col2\n1,2",
		},
		{
			"error_unsupportedFieldDataType",
			"#datatype ,whatever\n_measurement,col2\na,2",
		},
		{
			"error_unsupportedFieldValueDataType",
			"#datatype ,,whatever\n_measurement,_field,_value\na,1,2",
		},
		{
			"error_no_measurement_data",
			"_measurement,col1\n,2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := readCsv(t, test.csv)
			table := CsvTable{}
			var errors []error
			for _, row := range rows {
				rowProcessed := table.AddRow(row)
				if rowProcessed {
					_, err := table.CreateLine(row)
					if err != nil {
						errors = append(errors, err)
					}
				}
			}
			require.Equal(t, 1, len(errors))
			// fmt.Println(errors[0])
			require.NotNil(t, errors[0].Error())
			// LineLabel is the same as Label in all test columns
			for _, col := range table.Columns() {
				require.Equal(t, col.Label, col.LineLabel())
			}
		})
	}
}

// TestCsvData_dataErrors validates table data errors
func TestCsvData_logWarnings(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	var tests = []struct {
		name string
		csv  string
		log  string
	}{
		{
			"warning_unsupported_linepart",
			"#linepart ,,whatever\n_measurement,_field,_value\na,1,2",
			"unsupported line type: whatever",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf.Reset()
			rows := readCsv(t, test.csv)
			table := CsvTable{}
			for _, row := range rows {
				rowProcessed := table.AddRow(row)
				if rowProcessed {
					_, err := table.CreateLine(row)
					if err != nil {
						t.Fail()
					}
				}
			}
			out := buf.String()
			warningIndex := strings.Index(out, "WARNING:")
			require.Greater(t, warningIndex, 0)
			require.Equal(t, test.log, out[warningIndex+len("WARNING:")+1:len(out)-1])
		})
	}
}
