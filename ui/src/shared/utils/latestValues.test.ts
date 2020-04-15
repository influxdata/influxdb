import fromFlux from 'src/shared/utils/fromFlux.legacy'

import {latestValues} from 'src/shared/utils/latestValues'

describe('latestValues', () => {
  test('the last value returned does not depend on the ordering of tables in response', () => {
    const respA = `#group,false,false,false,false
#datatype,string,long,dateTime:RFC3339,long
#default,1,,,
,result,table,_time,_value
,,0,2018-12-10T18:29:48Z,1
,,0,2018-12-10T18:54:18Z,2

#group,false,false,false,false
#datatype,string,long,dateTime:RFC3339,long
#default,1,,,
,result,table,_time,_value
,,1,2018-12-10T18:29:48Z,3
,,1,2018-12-10T18:40:18Z,4`

    const respB = `#group,false,false,false,false
#datatype,string,long,dateTime:RFC3339,long
#default,1,,,
,result,table,_time,_value
,,0,2018-12-10T18:29:48Z,3
,,0,2018-12-10T18:40:18Z,4

#group,false,false,false,false
#datatype,string,long,dateTime:RFC3339,long
#default,1,,,
,result,table,_time,_value
,,1,2018-12-10T18:29:48Z,1
,,1,2018-12-10T18:54:18Z,2`

    const latestValuesA = latestValues(fromFlux(respA).table)
    const latestValuesB = latestValues(fromFlux(respB).table)

    expect(latestValuesA).toEqual([2])
    expect(latestValuesB).toEqual([2])
  })

  test('uses the latest time for which a value is defined', () => {
    const resp = `#group,false,false,false,false
#datatype,string,long,dateTime:RFC3339,long
#default,1,,,
,result,table,_time,_value
,,0,2018-12-10T18:29:48Z,3
,,0,2018-12-10T18:40:18Z,4

#group,false,false,false,false
#datatype,string,long,dateTime:RFC3339,string
#default,1,,,
,result,table,_time,_value
,,1,2018-12-10T19:00:00Z,howdy
,,1,2018-12-10T20:00:00Z,howdy`

    const result = latestValues(fromFlux(resp).table)

    expect(result).toEqual(['howdy'])
  })

  test('falls back to _stop column if _time column does not exist', () => {
    const resp = `#group,false,false,true,true,false
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,long
#default,1,,,,
,result,table,_start,_stop,_value
,,0,2018-12-10T18:29:48Z,2018-12-10T18:29:48Z,3
,,0,2018-12-10T18:40:18Z,2018-12-10T18:40:18Z,4`

    const result = latestValues(fromFlux(resp).table)

    expect(result).toEqual([4])
  })

  test('returns no latest values if no time column exists and multiple rows', () => {
    const resp = `#group,false,false,false
#datatype,string,long,long
#default,1,,
,result,table,_value
,,0,3
,,0,4`

    const result = latestValues(fromFlux(resp).table)

    expect(result).toEqual([])
  })

  test('returns latest values if no time column exists but table has single row', () => {
    const resp = `#group,false,false,false,false
#datatype,string,long,long,long
#default,1,,,
,result,table,_value,foo
,,0,3,4`

    const result = latestValues(fromFlux(resp).table)

    expect(result).toEqual([3, 4])
  })

  test('returns string values if no numeric column exists', () => {
    const resp = `#group,false,false,false,false
#datatype,string,long,dateTime:RFC3339,string
#default,1,,,
,result,table,_time,_value
,,1,2018-12-10T19:00:00Z,howdy
,,1,2018-12-10T20:00:00Z,howdy`

    const result = latestValues(fromFlux(resp).table)

    expect(result).toEqual(['howdy'])
  })

  test('returns latest values from multiple numeric value columns', () => {
    const resp = `#group,false,false,false,false,false
#datatype,string,long,dateTime:RFC3339,long,double
#default,1,,,,
,result,table,_time,_value,foo
,,0,2018-12-10T18:29:48Z,3,5.0
,,0,2018-12-10T18:40:18Z,4,6.0

#group,false,false,false,false,false
#datatype,string,long,dateTime:RFC3339,long,double
#default,1,,,,
,result,table,_time,_value,foo
,,0,2018-12-10T18:29:48Z,1,7.0
,,0,2018-12-10T18:40:18Z,2,8.0`

    const table = fromFlux(resp).table
    const result = latestValues(table)

    expect(result).toEqual([4, 6.0, 2.0, 8.0])
  })

  test('ignores rows with empty values', () => {
    const resp = `#group,false,false,true,true,true,true,true,false,false
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string,string,double,dateTime:RFC3339
#default,mean,,,,,,,,
,result,table,_start,_stop,_field,_measurement,host,_value,_time
,,0,2019-07-23T16:59:55.077422828Z,2019-07-23T17:04:55.077422828Z,used_percent,mem,oox4k.local,51.3,2019-07-23T17:04:00Z
,,0,2019-07-23T16:59:55.077422828Z,2019-07-23T17:04:55.077422828Z,used_percent,mem,oox4k.local,,2019-07-23T17:04:45Z`

    const table = fromFlux(resp).table
    const result = latestValues(table)

    expect(result).toEqual(['used_percent', 'mem', 'oox4k.local'])
  })
})
