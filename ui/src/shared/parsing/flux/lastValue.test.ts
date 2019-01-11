import {lastValue} from 'src/shared/parsing/flux/lastValue'
import {parseResponse} from 'src/shared/parsing/flux/response'

describe('lastValue', () => {
  test('the last value returned does not depend on the ordering of series', () => {
    const respA = `#group,false,false,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,0,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-12-10T18:21:52.748859Z,2018-12-10T18:30:00Z,2018-12-10T18:29:58Z,1,active,mem,oox4k.local
,,0,2018-12-10T18:30:00Z,2018-12-10T19:00:00Z,2018-12-10T18:54:08Z,2,active,mem,oox4k.local

#group,false,false,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,1,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-12-10T18:21:52.748859Z,2018-12-10T18:30:00Z,2018-12-10T18:29:48Z,3,active,mem,oox4k.local
,,0,2018-12-10T18:30:00Z,2018-12-10T19:00:00Z,2018-12-10T18:40:18Z,4,active,mem,oox4k.local

`

    const respB = `#group,false,false,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,1,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-12-10T18:21:52.748859Z,2018-12-10T18:30:00Z,2018-12-10T18:29:48Z,3,active,mem,oox4k.local
,,0,2018-12-10T18:30:00Z,2018-12-10T19:00:00Z,2018-12-10T18:40:18Z,4,active,mem,oox4k.local

#group,false,false,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,0,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-12-10T18:21:52.748859Z,2018-12-10T18:30:00Z,2018-12-10T18:29:58Z,1,active,mem,oox4k.local
,,0,2018-12-10T18:30:00Z,2018-12-10T19:00:00Z,2018-12-10T18:54:08Z,2,active,mem,oox4k.local

`

    const lastValueA = lastValue(parseResponse(respA))
    const lastValueB = lastValue(parseResponse(respB))

    expect(lastValueA).toEqual(2)
    expect(lastValueB).toEqual(2)
  })
})
