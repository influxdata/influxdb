package universe_test

import "testing"
import "testing/expect"
import "planner"
import "csv"
import "experimental/geo"

option now = () => 2030-01-01T00:00:00Z

testcase geo_merge_filter {
    input = "
#group,false,false,false,true,false,false,false
#datatype,string,long,dateTime:RFC3339,string,string,double,double
#default,_result,,,,,,
,result,table,_time,_measurement,id,lat,lon
,,0,2021-05-02T11:37:40Z,the_measurement,us7000dzhg,-30.133,-71.5399
"
    want = csv.from(
        csv: "
#group,false,false,true,true,false,true,false,false,false
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double,double
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_measurement,id,lat,lon
,,0,1930-01-01T00:00:00Z,2030-01-01T00:00:00Z,2021-05-02T11:37:40Z,the_measurement,us7000dzhg,-30.133,-71.5399
",
    )
    result = csv.from(csv: input)
        |> range(start: -100y)
	|> filter(fn: (r) => r["_measurement"] == "the_measurement")
	|> geo.strictFilter(region: { lat: -30.000, lon: -71.0000, radius: 100.0 })

    testing.diff(want: want, got: result)
}
