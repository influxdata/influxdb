import {toMinardTable} from 'src/shared/utils/toMinardTable'
import {parseResponse} from 'src/shared/parsing/flux/response'

describe('toMinardTable', () => {
  test('with basic data', () => {
    const CSV = `#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:33Z,10,usage_guest,cpu,cpu-total,oox4k.local
,,0,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:43Z,20,usage_guest,cpu,cpu-total,oox4k.local

#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,1,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:33Z,30,usage_guest,cpu,cpu0,oox4k.local
,,1,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:43Z,40,usage_guest,cpu,cpu0,oox4k.local`

    const tables = parseResponse(CSV)
    const actual = toMinardTable(tables)
    const expected = {
      schemaConflicts: [],
      table: {
        columnTypes: {
          _field: 'categorical',
          _measurement: 'categorical',
          _start: 'temporal',
          _stop: 'temporal',
          _time: 'temporal',
          _value: 'numeric',
          cpu: 'categorical',
          host: 'categorical',
          result: 'categorical',
        },
        columns: {
          _field: ['usage_guest', 'usage_guest', 'usage_guest', 'usage_guest'],
          _measurement: ['cpu', 'cpu', 'cpu', 'cpu'],
          _start: [1549064312524, 1549064312524, 1549064312524, 1549064312524],
          _stop: [1549064342524, 1549064342524, 1549064342524, 1549064342524],
          _time: [1549064313000, 1549064323000, 1549064313000, 1549064323000],
          _value: [10, 20, 30, 40],
          cpu: ['cpu-total', 'cpu-total', 'cpu0', 'cpu0'],
          host: ['oox4k.local', 'oox4k.local', 'oox4k.local', 'oox4k.local'],
          result: ['_result', '_result', '_result', '_result'],
        },
      },
    }

    expect(actual).toEqual(expected)
  })

  test('with a schema conflict', () => {
    const CSV = `#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:33Z,10,usage_guest,cpu,cpu-total,oox4k.local
,,0,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:43Z,20,usage_guest,cpu,cpu-total,oox4k.local

#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,1,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:33Z,30,usage_guest,cpu,cpu0,oox4k.local
,,1,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:43Z,40,usage_guest,cpu,cpu0,oox4k.local`

    const tables = parseResponse(CSV)
    const actual = toMinardTable(tables)
    const expected = {
      schemaConflicts: ['_value'],
      table: {
        columnTypes: {
          _field: 'categorical',
          _measurement: 'categorical',
          _start: 'temporal',
          _stop: 'temporal',
          _time: 'temporal',
          _value: 'numeric',
          cpu: 'categorical',
          host: 'categorical',
          result: 'categorical',
        },
        columns: {
          _field: ['usage_guest', 'usage_guest', 'usage_guest', 'usage_guest'],
          _measurement: ['cpu', 'cpu', 'cpu', 'cpu'],
          _start: [1549064312524, 1549064312524, 1549064312524, 1549064312524],
          _stop: [1549064342524, 1549064342524, 1549064342524, 1549064342524],
          _time: [1549064313000, 1549064323000, 1549064313000, 1549064323000],
          _value: [10, 20, undefined, undefined],
          cpu: ['cpu-total', 'cpu-total', 'cpu0', 'cpu0'],
          host: ['oox4k.local', 'oox4k.local', 'oox4k.local', 'oox4k.local'],
          result: ['_result', '_result', '_result', '_result'],
        },
      },
    }

    expect(actual).toEqual(expected)
  })
})
