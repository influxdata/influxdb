import fromFlux from './fromFlux'

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

    expect(actual.table.columns['result'].data).toEqual([
      '_result',
      '_result',
      '_result',
      '_result',
    ])

    expect(actual.table.columns['_start'].data).toEqual([
      1549064312524,
      1549064312524,
      1549064312524,
      1549064312524,
    ])

    expect(actual.table.columns['_stop'].data).toEqual([
      1549064342524,
      1549064342524,
      1549064342524,
      1549064342524,
    ])

    expect(actual.table.columns['_time'].data).toEqual([
      1549064313000,
      1549064323000,
      1549064313000,
      1549064323000,
    ])

    expect(actual.table.columns['_value (number)'].data).toEqual([
      10,
      20,
      undefined,
      undefined,
    ])

    expect(actual.table.columns['_value (string)'].data).toEqual([
      undefined,
      undefined,
      'thirty',
      'fourty',
    ])

    expect(actual.table.columns['_field'].data).toEqual([
      'usage_guest',
      'usage_guest',
      'usage_guest',
      'usage_guest',
    ])

    expect(actual.table.columns['_measurement'].data).toEqual([
      'cpu',
      'cpu',
      'cpu',
      'cpu',
    ])

    expect(actual.table.columns['cpu'].data).toEqual([
      'cpu-total',
      'cpu-total',
      'cpu0',
      'cpu0',
    ])

    expect(actual.table.columns['host'].data).toEqual([
      'oox4k.local',
      'oox4k.local',
      'oox4k.local',
      'oox4k.local',
    ])

    expect(actual.table.columns['table'].data).toEqual([0, 1, 2, 3])

    //expect(actual.table.getColumnName('_value (number)')).toEqual('_value')

    //expect(actual.table.getColumnName('_value (string)')).toEqual('_value')

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

    expect(actual.columns['result'].data).toEqual(['_result', '_result'])
    expect(actual.columns['a'].data).toEqual(['usage_guest', 'usage_guest'])
    expect(actual.columns['b'].data).toEqual(['cpu', 'cpu'])
    expect(actual.columns['c'].data).toEqual([4, 5])
    expect(actual.columns['d'].data).toEqual([6, 6])
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

  test('parses empty numeric values as null', () => {
    const CSV = `#group,false,false,true,true,false,true,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:33Z,10,usage_guest,cpu,cpu-total,oox4k.local
,,1,2019-02-01T23:38:32.524234Z,2019-02-01T23:39:02.524234Z,2019-02-01T23:38:43Z,,usage_guest,cpu,cpu-total,oox4k.local`

    const {table} = fromFlux(CSV)

    expect(table.columns['_value'].data).toEqual([10, null])
  })

  test('handles newlines inside string values', () => {
    const CSV = `#group,false,false,false,false
#datatype,string,long,string,long
#default,_result,,,
,result,table,message,value
,,0,howdy,5
,,0,"hello

there",5
,,0,hi,6

#group,false,false,false,false
#datatype,string,long,string,long
#default,_result,,,
,result,table,message,value
,,1,howdy,5
,,1,"hello

there",5
,,1,hi,6`

    const {table} = fromFlux(CSV)

    expect(table.columns['value'].data).toEqual([5, 5, 6, 5, 5, 6])

    expect(table.columns['message'].data).toEqual([
      'howdy',
      'hello\n\nthere',
      'hi',
      'howdy',
      'hello\n\nthere',
      'hi',
    ])
  })

  test('handles errors that show up on any page', () => {
    const CSV = `#group,true,false,false,true
#datatype,string,string,string,string
#default,,,,
,a,b,c,d

#group,false,false,true,false
#datatype,string,string,string,string
#default,,,,
,error,reference
,query terminated: reached maximum allowed memory limits,576`

    expect(() => fromFlux(CSV)).toThrowError(
      '[576] query terminated: reached maximum allowed memory limits'
    )
  })
})
