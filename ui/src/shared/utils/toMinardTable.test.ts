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
      table: {
        columns: {
          result: {
            data: ['_result', '_result', '_result', '_result'],
            type: 'string',
          },
          _start: {
            data: [1549064312524, 1549064312524, 1549064312524, 1549064312524],
            type: 'time',
          },
          _stop: {
            data: [1549064342524, 1549064342524, 1549064342524, 1549064342524],
            type: 'time',
          },
          _time: {
            data: [1549064313000, 1549064323000, 1549064313000, 1549064323000],
            type: 'time',
          },
          _value: {data: [10, 20, 30, 40], type: 'float'},
          _field: {
            data: ['usage_guest', 'usage_guest', 'usage_guest', 'usage_guest'],
            type: 'string',
          },
          _measurement: {data: ['cpu', 'cpu', 'cpu', 'cpu'], type: 'string'},
          cpu: {
            data: ['cpu-total', 'cpu-total', 'cpu0', 'cpu0'],
            type: 'string',
          },
          host: {
            data: ['oox4k.local', 'oox4k.local', 'oox4k.local', 'oox4k.local'],
            type: 'string',
          },
        },
        length: 4,
      },
      schemaConflicts: [],
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
      table: {
        columns: {
          result: {
            data: ['_result', '_result', '_result', '_result'],
            type: 'string',
          },
          _start: {
            data: [1549064312524, 1549064312524, 1549064312524, 1549064312524],
            type: 'time',
          },
          _stop: {
            data: [1549064342524, 1549064342524, 1549064342524, 1549064342524],
            type: 'time',
          },
          _time: {
            data: [1549064313000, 1549064323000, 1549064313000, 1549064323000],
            type: 'time',
          },
          _value: {data: [10, 20, undefined, undefined], type: 'float'},
          _field: {
            data: ['usage_guest', 'usage_guest', 'usage_guest', 'usage_guest'],
            type: 'string',
          },
          _measurement: {data: ['cpu', 'cpu', 'cpu', 'cpu'], type: 'string'},
          cpu: {
            data: ['cpu-total', 'cpu-total', 'cpu0', 'cpu0'],
            type: 'string',
          },
          host: {
            data: ['oox4k.local', 'oox4k.local', 'oox4k.local', 'oox4k.local'],
            type: 'string',
          },
        },
        length: 4,
      },
      schemaConflicts: ['_value'],
    }

    expect(actual).toEqual(expected)
  })
})
