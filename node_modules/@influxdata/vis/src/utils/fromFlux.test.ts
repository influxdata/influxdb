import {fromFlux} from './fromFlux'

describe('fromFlux', () => {
  test('can parse a Flux CSV with mismatched schemas', () => {
    const CSV = `#group,false,false,true,true,false,true,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:33Z,10,usage_guest,cpu,cpu-total,oox4k.local
,,1,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:43Z,20,usage_guest,cpu,cpu-total,oox4k.local

#group,false,false,true,true,false,true,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,2,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:33Z,thirty,usage_guest,cpu,cpu0,oox4k.local
,,3,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:43Z,fourty,usage_guest,cpu,cpu0,oox4k.local`

    const actual = fromFlux(CSV)

    expect(actual.table.getColumn('result', 'string')).toEqual([
      '_result',
      '_result',
      '_result',
      '_result',
    ])

    expect(actual.table.getColumn('_start', 'time')).toEqual([
      1549064312524,
      1549064312524,
      1549064312524,
      1549064312524,
    ])

    expect(actual.table.getColumn('_stop', 'time')).toEqual([
      1549064342524,
      1549064342524,
      1549064342524,
      1549064342524,
    ])

    expect(actual.table.getColumn('_time', 'time')).toEqual([
      1549064313000,
      1549064323000,
      1549064313000,
      1549064323000,
    ])

    expect(actual.table.getColumn('_value (number)', 'number')).toEqual([
      10,
      20,
      undefined,
      undefined,
    ])

    expect(actual.table.getColumn('_value (string)', 'string')).toEqual([
      undefined,
      undefined,
      'thirty',
      'fourty',
    ])

    expect(actual.table.getColumn('_field', 'string')).toEqual([
      'usage_guest',
      'usage_guest',
      'usage_guest',
      'usage_guest',
    ])

    expect(actual.table.getColumn('_measurement', 'string')).toEqual([
      'cpu',
      'cpu',
      'cpu',
      'cpu',
    ])

    expect(actual.table.getColumn('cpu', 'string')).toEqual([
      'cpu-total',
      'cpu-total',
      'cpu0',
      'cpu0',
    ])

    expect(actual.table.getColumn('host', 'string')).toEqual([
      'oox4k.local',
      'oox4k.local',
      'oox4k.local',
      'oox4k.local',
    ])

    expect(actual.table.getColumn('table', 'number')).toEqual([0, 1, 2, 3])

    expect(actual.table.getColumnName('_value (number)')).toEqual('_value')

    expect(actual.table.getColumnName('_value (string)')).toEqual('_value')

    expect(actual.fluxGroupKeyUnion).toEqual([
      '_value (number)',
      '_value (string)',
      '_start',
      '_stop',
      '_field',
      '_measurement',
      'cpu',
      'host',
    ])
  })

  test('uses the default annotation to fill in empty values', () => {
    const CSV = `#group,false,false,true,true,true,true
#datatype,string,long,string,string,long,long
#default,_result,,,cpu,,6
,result,table,a,b,c,d
,,1,usage_guest,,4,
,,1,usage_guest,,5,`

    const actual = fromFlux(CSV).table

    expect(actual.getColumn('result')).toEqual(['_result', '_result'])
    expect(actual.getColumn('a')).toEqual(['usage_guest', 'usage_guest'])
    expect(actual.getColumn('b')).toEqual(['cpu', 'cpu'])
    expect(actual.getColumn('c')).toEqual([4, 5])
    expect(actual.getColumn('d')).toEqual([6, 6])
  })

  test('returns a group key union', () => {
    const CSV = `#group,true,false,false,true
#datatype,string,string,string,string
#default,,,,
,a,b,c,d

#group,false,false,true,false
#datatype,string,string,string,string
#default,,,,
,a,b,c,d`

    const {fluxGroupKeyUnion} = fromFlux(CSV)

    expect(fluxGroupKeyUnion).toEqual(['a', 'c', 'd'])
  })
})
