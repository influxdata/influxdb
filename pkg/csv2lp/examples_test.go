package csv2lp

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type csvExample struct {
	name string
	csv  string
	lp   string
}

var examples []csvExample = []csvExample{
	{
		"fluxQueryResult",
		`
#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:17:57Z,0,time_steal,cpu,cpu1,rsavage.prod
,,0,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:18:07Z,0,time_steal,cpu,cpu1,rsavage.prod

#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,1,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:18:01Z,2.7263631815907954,usage_user,cpu,cpu-total,tahoecity.prod
,,1,2020-02-25T22:17:54.068926364Z,2020-02-25T22:22:54.068926364Z,2020-02-25T22:18:11Z,2.247752247752248,usage_user,cpu,cpu-total,tahoecity.prod
`,
		`
cpu,cpu=cpu1,host=rsavage.prod time_steal=0 1582669077000000000
cpu,cpu=cpu1,host=rsavage.prod time_steal=0 1582669087000000000
cpu,cpu=cpu-total,host=tahoecity.prod usage_user=2.7263631815907954 1582669081000000000
cpu,cpu=cpu-total,host=tahoecity.prod usage_user=2.247752247752248 1582669091000000000
`,
	},
	{
		"annotatedSimple",
		`
#datatype measurement,tag,tag,double,double,ignored,dateTime:number
m,cpu,host,time_steal,usage_user,nothing,time
cpu,cpu1,rsavage.prod,0,2.7,a,1482669077000000000
cpu,cpu1,rsavage.prod,0,2.2,b,1482669087000000000
`,
		`
cpu,cpu=cpu1,host=rsavage.prod time_steal=0,usage_user=2.7 1482669077000000000
cpu,cpu=cpu1,host=rsavage.prod time_steal=0,usage_user=2.2 1482669087000000000
`,
	},
	{
		"annotatedSimple_labels",
		`
m|measurement,cpu|tag,host|tag,time_steal|double,usage_user|double,nothing|ignored,time|dateTime:number
cpu,cpu1,rsavage.prod,0,2.7,a,1482669077000000000
cpu,cpu1,rsavage.prod,0,2.2,b,1482669087000000000
`,
		`
cpu,cpu=cpu1,host=rsavage.prod time_steal=0,usage_user=2.7 1482669077000000000
cpu,cpu=cpu1,host=rsavage.prod time_steal=0,usage_user=2.2 1482669087000000000
`,
	},
	{
		"annotatedDatatype",
		`
#datatype measurement,tag,string,double,boolean,long,unsignedLong,duration,dateTime
#default test,annotatedDatatypes,,,,,,
m,name,s,d,b,l,ul,dur,time
,,str1,1.0,true,1,1,1ms,1
,,str2,2.0,false,2,2,2us,2020-01-11T10:10:10Z
`,
		`
test,name=annotatedDatatypes s="str1",d=1,b=true,l=1i,ul=1u,dur=1000000i 1
test,name=annotatedDatatypes s="str2",d=2,b=false,l=2i,ul=2u,dur=2000i 1578737410000000000
`,
	},
	{
		"annotatedDatatype_labels",
		`
m|measurement|test,name|tag|annotatedDatatypes,s|string,d|double,b|boolean,l|long,ul|unsignedLong,dur|duration,time|dateTime
,,str1,1.0,true,1,1,1ms,1
,,str2,2.0,false,2,2,2us,2020-01-11T10:10:10Z`,
		`
test,name=annotatedDatatypes s="str1",d=1,b=true,l=1i,ul=1u,dur=1000000i 1
test,name=annotatedDatatypes s="str2",d=2,b=false,l=2i,ul=2u,dur=2000i 1578737410000000000
`,
	},
	{
		"datetypeFormats",
		`
#constant measurement,test
#constant tag,name,datetypeFormats
#timezone -0500
#datatype dateTime:2006-01-02|1970-01-02,"double:,. ","boolean:y,Y:n,N|y"
t,d,b
1970-01-01,"123.456,78",
,"123 456,78",Y
`,
		`
test,name=datetypeFormats d=123456.78,b=true 18000000000000
test,name=datetypeFormats d=123456.78,b=true 104400000000000
`,
	},
	{
		"datetypeFormats_labels",
		`
#constant measurement,test
#constant tag,name,datetypeFormats
#timezone -0500
t|dateTime:2006-01-02|1970-01-02,"d|double:,. ","b|boolean:y,Y:n,N|y"
1970-01-01,"123.456,78",
,"123 456,78",Y
`,
		`
test,name=datetypeFormats d=123456.78,b=true 18000000000000
test,name=datetypeFormats d=123456.78,b=true 104400000000000
`,
	},
	{
		"datetypeFormats_labels_override",
		`
#constant measurement,test2
t|dateTime:2006-01-02,_|ignored,s|string|unknown
1970-01-01,"123.456,78",
,"123 456,78",Y
`,
		`
test2 s="unknown" 0
test2 s="Y"
`,
	},
	{
		"datetypeFormats_labels_override",
		`
m|measurement,usage_user|double
cpu,2.7
cpu,nil
cpu,
,2.9
`,
		`
cpu usage_user=2.7
`,
	},
	{
		"columnSeparator",
		`sep=;
m|measurement;available|boolean:y,Y:|n;dt|dateTime:number
test;nil;1
test;N;2
test;";";3
test;;4
test;Y;5
`,
		`
test available=false 1
test available=false 2
test available=false 3
test available=false 4
test available=true 5
`,
	},
}

func (example *csvExample) normalize() rune {
	for len(example.lp) > 0 && example.lp[0] == '\n' {
		example.lp = example.lp[1:]
	}
	if strings.HasPrefix(example.csv, "sep=") {
		return (rune)(example.csv[4])
	}
	return ','
}

// Test_Examples tests examples of README.md file herein
func Test_Examples(t *testing.T) {
	for _, example := range examples {
		t.Run(example.name, func(t *testing.T) {
			comma := example.normalize()
			transformer := CsvToLineProtocol(strings.NewReader(example.csv))
			transformer.SkipRowOnError(true)
			result, err := ioutil.ReadAll(transformer)
			if err != nil {
				require.Nil(t, fmt.Sprintf("%s", err))
			}
			require.Equal(t, comma, transformer.Comma())
			require.Equal(t, example.lp, string(result))
		})
	}
}
