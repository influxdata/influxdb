import {fluxTablesToDygraph} from 'src/shared/parsing/flux/dygraph'
import {parseResponse} from 'src/shared/parsing/flux/response'
import {
  SIMPLE,
  MISMATCHED,
  MULTI_VALUE_ROW,
  MIXED_DATATYPES,
} from 'src/shared/parsing/flux/constants'

describe('fluxTablesToDygraph', () => {
  it('can parse flux tables to dygraph series', () => {
    const fluxTables = parseResponse(SIMPLE)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = [
      [new Date('2018-09-10T22:34:29Z'), 0],
      [new Date('2018-09-10T22:34:39Z'), 10],
    ]

    expect(actual.dygraphsData).toEqual(expected)
  })

  it('can parse flux tables for series of mismatched periods', () => {
    const fluxTables = parseResponse(MISMATCHED)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = [
      [new Date('2018-06-04T17:12:25Z'), 1, null],
      [new Date('2018-06-04T17:12:35Z'), 2, null],
      [new Date('2018-06-05T17:12:25Z'), null, 10],
      [new Date('2018-06-05T17:12:35Z'), null, 11],
    ]

    expect(actual.dygraphsData).toEqual(expected)
  })

  it('can parse multiple values per row', () => {
    const fluxTables = parseResponse(MULTI_VALUE_ROW)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = {
      labels: [
        'time',
        'mean_usage_idle[result=0][_measurement=cpu]',
        'mean_usage_user[result=0][_measurement=cpu]',
        'mean_usage_idle[result=0][_measurement=mem]',
        'mean_usage_user[result=0][_measurement=mem]',
      ],
      dygraphsData: [
        [new Date('2018-09-10T16:54:37Z'), 85, 10, 8, 1],
        [new Date('2018-09-10T16:54:38Z'), 87, 7, 9, 2],
        [new Date('2018-09-10T16:54:39Z'), 89, 5, 10, 3],
      ],
      nonNumericColumns: [],
    }

    expect(actual).toEqual(expected)
  })

  it('filters out non-numeric series', () => {
    const fluxTables = parseResponse(MIXED_DATATYPES)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = {
      labels: [
        'time',
        'mean_usage_idle[result=0][_measurement=cpu]',
        'mean_usage_idle[result=0][_measurement=mem]',
      ],
      dygraphsData: [
        [new Date('2018-09-10T16:54:37Z'), 85, 8],
        [new Date('2018-09-10T16:54:39Z'), 89, 10],
      ],
      nonNumericColumns: ['my_fun_col'],
    }

    expect(actual).toEqual(expected)
  })

  it('can parse identical series in different results', () => {
    const resp = `#group,false,false,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,0,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-12-10T18:21:52.748859Z,2018-12-10T18:30:00Z,2018-12-10T18:29:58Z,4906213376,active,mem,oox4k.local
,,0,2018-12-10T18:30:00Z,2018-12-10T19:00:00Z,2018-12-10T18:54:08Z,5860683776,active,mem,oox4k.local

#group,false,false,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,1,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-12-10T18:21:52.748859Z,2018-12-10T18:30:00Z,2018-12-10T18:29:48Z,4589981696,active,mem,oox4k.local
,,0,2018-12-10T18:30:00Z,2018-12-10T19:00:00Z,2018-12-10T18:40:18Z,4318040064,active,mem,oox4k.local


`
    const fluxTables = parseResponse(resp)
    const actual = fluxTablesToDygraph(fluxTables)
    const expected = {
      dygraphsData: [
        [new Date('2018-12-10T18:29:48.000Z'), null, 4589981696],
        [new Date('2018-12-10T18:29:58.000Z'), 4906213376, null],
        [new Date('2018-12-10T18:40:18.000Z'), null, 4318040064],
        [new Date('2018-12-10T18:54:08.000Z'), 5860683776, null],
      ],
      labels: [
        'time',
        '_value[result=0][_field=active][_measurement=mem][host=oox4k.local]',
        '_value[result=1][_field=active][_measurement=mem][host=oox4k.local]',
      ],
      nonNumericColumns: [],
    }

    expect(actual).toEqual(expected)
  })
})
