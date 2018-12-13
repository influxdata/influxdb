import {parseResponse} from 'src/shared/parsing/flux/response'
import {
  RESPONSE_NO_METADATA,
  RESPONSE_METADATA,
  MULTI_SCHEMA_RESPONSE,
  EXPECTED_COLUMNS,
  TRUNCATED_RESPONSE,
} from 'src/shared/parsing/flux/constants'

describe('parseResponse', () => {
  test('parseResponse into the right number of tables', () => {
    const result = parseResponse(MULTI_SCHEMA_RESPONSE)
    expect(result).toHaveLength(4)
  })

  describe('result name', () => {
    test('uses the result name from the result column if present', () => {
      const resp = `#group,false,false,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,max,0,2018-12-10T18:21:52.748859Z,2018-12-10T18:30:00Z,2018-12-10T18:29:58Z,4906213376,active,mem,oox4k.local
,max,0,2018-12-10T18:30:00Z,2018-12-10T19:00:00Z,2018-12-10T18:54:08Z,5860683776,active,mem,oox4k.local
,max,0,2018-12-10T19:00:00Z,2018-12-10T19:21:52.748859Z,2018-12-10T19:11:58Z,5115428864,active,mem,oox4k.local

`

      const actual = parseResponse(resp)

      expect(actual[0].result).toBe('max')
    })

    test('uses the result name from the default annotation if result columns are empty', () => {
      const resp = `#group,false,false,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,max,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-12-10T18:21:52.748859Z,2018-12-10T18:30:00Z,2018-12-10T18:29:58Z,4906213376,active,mem,oox4k.local
,,0,2018-12-10T18:30:00Z,2018-12-10T19:00:00Z,2018-12-10T18:54:08Z,5860683776,active,mem,oox4k.local
,,0,2018-12-10T19:00:00Z,2018-12-10T19:21:52.748859Z,2018-12-10T19:11:58Z,5115428864,active,mem,oox4k.local

#group,false,false,false,false,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,min,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-12-10T18:21:52.748859Z,2018-12-10T18:30:00Z,2018-12-10T18:29:48Z,4589981696,active,mem,oox4k.local
,,0,2018-12-10T18:30:00Z,2018-12-10T19:00:00Z,2018-12-10T18:40:18Z,4318040064,active,mem,oox4k.local
,,0,2018-12-10T19:00:00Z,2018-12-10T19:21:52.748859Z,2018-12-10T19:13:58Z,4131692544,active,mem,oox4k.local


`
      const actual = parseResponse(resp)

      expect(actual).toHaveLength(2)
      expect(actual[0].result).toBe('max')
      expect(actual[1].result).toBe('min')
    })
  })

  describe('headers', () => {
    test('throws when no metadata is present', () => {
      expect(() => {
        parseResponse(RESPONSE_NO_METADATA)
      }).toThrow()
    })

    test('can parse headers when metadata is present', () => {
      const actual = parseResponse(RESPONSE_METADATA)[0].data[0]
      expect(actual).toEqual(EXPECTED_COLUMNS)
    })
  })

  describe('group key', () => {
    test('parses the group key properly', () => {
      const actual = parseResponse(MULTI_SCHEMA_RESPONSE)[0].groupKey
      const expected = {
        _field: 'usage_guest',
        _measurement: 'cpu',
        cpu: 'cpu-total',
        host: 'WattsInfluxDB',
      }
      expect(actual).toEqual(expected)
    })
  })

  describe('partial responses', () => {
    test('should discard tables without any non-annotation rows', () => {
      const actual = parseResponse(TRUNCATED_RESPONSE)

      expect(actual).toHaveLength(2)
    })
  })
})
