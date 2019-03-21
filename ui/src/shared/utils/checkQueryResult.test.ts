import {checkQueryResult} from 'src/shared/utils/checkQueryResult'

describe('checkQueryResult', () => {
  test('throws an error when the response has an error table', () => {
    const RESPONSE = `#group,true,true
#datatype,string,string
#default,,
,error,reference
,"function references unknown column ""_value""",`

    expect(() => {
      checkQueryResult(RESPONSE)
    }).toThrow('function references unknown column')
  })

  test('does not throw an error when the response is valid', () => {
    const RESPONSE = `#group,false,false,true,true,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_measurement,host,_field
,,0,2019-03-21T18:54:14.113478Z,2019-03-21T19:54:14.113478Z,2019-03-21T18:54:21Z,4780101632,mem,oox4k.local,active
,,0,2019-03-21T18:54:14.113478Z,2019-03-21T19:54:14.113478Z,2019-03-21T18:54:31Z,5095436288,mem,oox4k.local,active`

    expect(() => {
      checkQueryResult(RESPONSE)
    }).not.toThrow()
  })
})
