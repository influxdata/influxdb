package csv2lp

import (
	"encoding/csv"
	"io"
	"strconv"
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

// Test_CsvTable_FluxQueryResult tests construction of table columns and data from a Flux Query CSV result
func Test_CsvTable_FluxQueryResult(t *testing.T) {
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
						types := strings.Split(rows[i-3][j], ":")
						require.Equal(t, types[0], col.DataType, "row %d, col %d", i-3, j)
						if len(types) > 1 {
							require.Equal(t, types[1], col.DataFormat, "row %d, col %d", i-3, j)
						}
					}
				}
				// verify cached values
				table.computeLineProtocolColumns()
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
				require.Contains(t, table.ColumnLabels(), "_measurement")
			}
		}
	}
}

//Test_IgnoreLeadingComment tests ignoreLeadingComment fn
func Test_IgnoreLeadingComment(t *testing.T) {
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

// Test_CsvTableProcessing tests data processing in CsvTable
func Test_CsvTableProcessing(t *testing.T) {
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
			"#datatype measurement,,\nmeasurement,a,b\ncpu,1,2",
			"cpu a=1,b=2",
		},
		{
			"annotated2",
			"#datatype measurement,tag,field\nmeasurement,a,b\ncpu,1,2",
			"cpu,a=1 b=2",
		},
		{
			"annotated3",
			"#datatype measurement,tag,dateTime,field\nmeasurement,a,b,time\ncpu,1,2,3",
			"cpu,a=1 time=3 2",
		},
		{
			"annotated3_detectedTime1",
			"#datatype measurement,tag,dateTime,field\nmeasurement,a,b,time\ncpu,1,2020-01-10T10:10:10Z,3",
			"cpu,a=1 time=3 1578651010000000000",
		},
		{
			"annotated3_detectedTime2",
			"#datatype measurement,tag,dateTime,field\nmeasurement,a,b,time\ncpu,1,2020-01-10T10:10:10.0Z,3",
			"cpu,a=1 time=3 1578651010000000000",
		},
		{
			"annotated4",
			"#datatype measurement,tag,ignore,field\nmeasurement,a,b,time\ncpu,1,2,3",
			"cpu,a=1 time=3",
		},
		{
			"annotated5",
			"#datatype measurement,tag,ignore,field\nmeasurement,a,b,time\ncpu,1,2,3",
			"cpu,a=1 time=3",
		},
		{
			"annotated6",
			"#datatype measurement,tag,ignore,field\n" +
				"#datatypea tag,tag,\n" + // this must be ignored since it not a supported annotation
				"measurement,a,b,time\ncpu,1,2,3",
			"cpu,a=1 time=3",
		},
		{
			"annotated7",
			"#datatype measurement,dateTime,\nmeasurement,a,b\ncpu,2020-01-10T10:10:10.0Z,2",
			"cpu b=2 1578651010000000000",
		},
		{
			"annotated8",
			"#datatype measurement,,,field\nmeasurement,_field,_value,other\ncpu,a,1,2",
			"cpu a=1,other=2",
		},
		{
			"annotated9_sortedTags",
			"#datatype measurement,tag,tag,time,field\nmeasurement,b,a,c,time\ncpu,1,2,3,4",
			"cpu,a=2,b=1 time=4 3",
		},
		{
			"allFieldTypes",
			"#datatype measurement,string,double,boolean,long,unsignedLong,duration,base64Binary,dateTime\n" +
				"m,s,d,b,l,ul,dur,by,d1,d2,time\n" +
				`cpu,"str",1.0,true,1,1,1ms,YWFh,1`,
			"cpu s=\"str\",d=1,b=true,l=1i,ul=1u,dur=1000000i,by=YWFh 1",
		},
		{
			"allFieldTypes",
			"#datatype measurement,string,double,boolean,long,unsignedLong,duration,base64Binary,dateTime\n" +
				"m,s,d,b,l,ul,dur,by,d1,d2,time\n" +
				`cpu,"str",1.0,true,1,1,1ms,YWFh,1`,
			"cpu s=\"str\",d=1,b=true,l=1i,ul=1u,dur=1000000i,by=YWFh 1",
		},
		{
			"allFieldTypes_ignoreAdditionalDateTimes",
			"#datatype ,string,double,boolean,long,unsignedLong,duration,base64Binary,dateTime:RFC3339,dateTime:RFC3339Nano,\n" +
				"_measurement,s,d,b,l,ul,dur,by,d1,d2,_time\n" +
				`cpu,"str",1.0,true,1,1,1ms,YWFh,2020-01-10T10:10:10Z,2020-01-10T10:10:10Z,1`,
			"cpu s=\"str\",d=1,b=true,l=1i,ul=1u,dur=1000000i,by=YWFh 1",
		},
		{
			"allExtraDataTypes",
			"#datatype measurement,tag,field,ignored,dateTime\n" +
				"m,t,f,i,dt\n" +
				`cpu,myTag,0,myIgnored,1`,
			"cpu,t=myTag f=0 1",
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
			"#default cpu,yes,0,1\n#datatype ,tag,,\n_measurement,test,col1,_time\n,,,",
			"cpu,test=yes col1=0 1",
		},
		{
			"no duplicate tags", // duplicate tags are ignored, the last column wins, https://github.com/influxdata/influxdb/issues/19453
			"#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string,string,string,string\n" +
				"#group,true,true,false,false,false,false,true,true,true,true,true,true,true,true,true,true\n" +
				"#default,_result,,,,,,,,,,,,,,,\n" +
				",result,table,_start,_stop,_time,_value,_field,_measurement,env,host,hostname,nodename,org,result,table,url\n" +
				",,0,2020-08-26T23:10:54.023607624Z,2020-08-26T23:15:54.023607624Z,2020-08-26T23:11:00Z,0,0.001,something,host,pod,node,host,,success,role,http://127.0.0.1:8099/metrics\n",
			"something,env=host,host=pod,hostname=node,nodename=host,result=success,table=role,url=http://127.0.0.1:8099/metrics 0.001=0 1598483460000000000",
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

// Test_ConstantAnnotations tests processing of constant annotations
func Test_ConstantAnnotations(t *testing.T) {
	var tests = []struct {
		name string
		csv  string
		line string
	}{
		{
			"measurement_1",
			"#constant measurement,cpu\n" +
				"a,b\n" +
				"1,1",
			"cpu a=1,b=1",
		},
		{
			"measurement_2",
			"#constant,measurement,,cpu\n" +
				"#constant,tag,cpu,cpu1\n" +
				"#constant,long,of,0\n" +
				"#constant,dateTime,,2\n" +
				"a,b\n" +
				"1,1",
			"cpu,cpu=cpu1 a=1,b=1,of=0i 2",
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

// Test_ConcatAnnotations tests processing of concat annotations
func Test_ConcatAnnotations(t *testing.T) {
	var tests = []struct {
		name string
		csv  string
		line string
	}{
		{
			"measurement_1",
			"#concat measurement,cpu\n" +
				"a,b\n" +
				"1,1",
			"cpu a=1,b=1",
		},
		{
			"measurement_2",
			"#concat,measurement,${a}${b}\n" +
				"#constant,tag,cpu,cpu1\n" +
				"#constant,long,of,0\n" +
				"#constant,dateTime,,2\n" +
				"a,b\n" +
				"1,1",
			"11,cpu=cpu1 a=1,b=1,of=0i 2",
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

// Test_DataTypeInColumnName tests specification of column data type in the header row
func Test_DataTypeInColumnName(t *testing.T) {
	var tests = []struct {
		csv                        string
		line                       string
		ignoreDataTypeInColumnName bool
		error                      string
	}{
		{
			csv: "m|measurement,b|boolean:x:,c|boolean:x:|x\n" +
				"cpu,,",
			line: `cpu c=true`,
		},
		{
			csv: "m|measurement,a|boolean,b|boolean:0:1,c|boolean:x:,d|boolean:x:\n" +
				"cpu,1,1,x,y",
			line: `cpu a=true,b=false,c=true,d=false`,
		},
		{
			csv: "#constant measurement,cpu\n" +
				"a|long,b|string\n" +
				"1,1",
			line: `cpu a=1i,b="1"`,
		},
		{
			csv: "#constant measurement,cpu\n" +
				"a|long,b|string\n" +
				"1,1",
			line:                       `cpu a|long=1,b|string=1`,
			ignoreDataTypeInColumnName: true,
		},
		{
			csv: "#constant measurement,cpu\n" +
				"#datatype long,string\n" +
				"a|long,b|string\n" +
				"1,1",
			line:                       `cpu a|long=1i,b|string="1"`,
			ignoreDataTypeInColumnName: true,
		},
		{
			csv: "#constant measurement,cpu\n" +
				"a|long:strict: ,b|unsignedLong:strict: \n" +
				"1 2,1 2",
			line: `cpu a=12i,b=12u`,
		},
		{
			csv: "#constant measurement,cpu\n" +
				"a|long:strict\n" +
				"1.1,1",
			error: "column 'a': '1.1' cannot fit into long data type",
		},
		{
			csv: "#constant measurement,cpu\n" +
				"a|unsignedLong:strict\n" +
				"1.1,1",
			error: "column 'a': '1.1' cannot fit into unsignedLong data type",
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			rows := readCsv(t, test.csv)
			table := CsvTable{}
			table.IgnoreDataTypeInColumnName(test.ignoreDataTypeInColumnName)
			var lines []string
			for _, row := range rows {
				rowProcessed := table.AddRow(row)
				if rowProcessed {
					line, err := table.CreateLine(row)
					if err != nil {
						if test.error == "" {
							require.Nil(t, err.Error())
						} else {
							require.Equal(t, test.error, err.Error())
						}
					}
					lines = append(lines, line)
				}
			}
			require.Equal(t, []string{test.line}, lines)
		})
	}
}

// Test_CsvTable_dataErrors tests reporting of table data errors
func Test_CsvTable_dataErrors(t *testing.T) {
	var tests = []struct {
		name string
		csv  string
	}{
		{
			"error_1_is_not_dateTime:RFC3339",
			"#datatype measurement,,\n#datatype ,dateTime:RFC3339,\nmeasurement,a,b\ncpu,1,2",
		},
		{
			"error_a_fieldValue_is_not_long",
			"#datatype measurement,,\n#datatype ,long,\nmeasurement,_value,_field\ncpu,a,count",
		},
		{
			"error_a_is_not_long",
			"#datatype measurement,,\n#datatype ,long,\nmeasurement,a,b\ncpu,a,2",
		},
		{
			"error_time_is_not_time",
			"#datatype measurement,tag,time,field\nmeasurement,a,b,time\ncpu,1,2020-10,3",
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
		{
			"error_derived_column_missing reference",
			"#concat string,d,${col1}${col2}\n_measurement,col1\nm,2",
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
			// LineLabel is the same as Label in all tested columns
			for _, col := range table.Columns() {
				require.Equal(t, col.Label, col.LineLabel())
			}
		})
	}
}

// Test_CsvTable_DataColumnsInfo tests reporting of table columns
func Test_CsvTable_DataColumnsInfo(t *testing.T) {
	data := "#constant,measurement,cpu\n" +
		"#constant,tag,xpu,xpu1\n" +
		"#constant,tag,cpu,cpu1\n" +
		"#constant,long,of,100\n" +
		"#constant,dateTime,2\n" +
		"x,y\n"
	table := CsvTable{}
	for _, row := range readCsv(t, data) {
		require.False(t, table.AddRow(row))
	}
	table.computeLineProtocolColumns()
	// expected result is something like this:
	// "CsvTable{ dataColumns: 2 constantColumns: 5\n" +
	// 	" measurement: &{Label:#constant measurement DataType:measurement DataFormat: LinePart:2 DefaultValue:cpu Index:-1 TimeZone:UTC ParseF:<nil> escapedLabel:}\n" +
	// 	" tag:         {Label:cpu DataType:tag DataFormat: LinePart:3 DefaultValue:cpu1 Index:-1 TimeZone:UTC ParseF:<nil> escapedLabel:cpu}\n" +
	// 	" tag:         {Label:xpu DataType:tag DataFormat: LinePart:3 DefaultValue:xpu1 Index:-1 TimeZone:UTC ParseF:<nil> escapedLabel:xpu}\n" +
	// 	" field:       {Label:x DataType: DataFormat: LinePart:0 DefaultValue: Index:0 TimeZone:UTC ParseF:<nil> escapedLabel:x}\n" +
	// 	" field:       {Label:y DataType: DataFormat: LinePart:0 DefaultValue: Index:1 TimeZone:UTC ParseF:<nil> escapedLabel:y}\n" +
	// 	" field:       {Label:of DataType:long DataFormat: LinePart:0 DefaultValue:100 Index:-1 TimeZone:UTC ParseF:<nil> escapedLabel:of}\n" +
	// 	" time:        &{Label:#constant dateTime DataType:dateTime DataFormat: LinePart:5 DefaultValue:2 Index:-1 TimeZone:UTC ParseF:<nil> escapedLabel:}" +
	// 	"\n}"
	result := table.DataColumnsInfo()
	require.Equal(t, 1, strings.Count(result, "measurement:"))
	require.Equal(t, 2, strings.Count(result, "tag:"))
	require.Equal(t, 3, strings.Count(result, "field:"))
	require.Equal(t, 1, strings.Count(result, "time:"))

	var table2 *CsvTable
	require.Equal(t, "<nil>", table2.DataColumnsInfo())
}
