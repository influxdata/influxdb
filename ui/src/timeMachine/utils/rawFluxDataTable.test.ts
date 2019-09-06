import {parseFiles} from './rawFluxDataTable'

describe('parseFiles', () => {
  test('can parse multi-csv response', () => {
    const CSV = `
#group,false,false,false,false
#datatype,string,long,string,long
#default,_result,,,
,result,table,message,value
,,0,howdy,5
,,0,hello there,5
,,0,hi,6

#group,false,false,false,false
#datatype,string,long,string,long
#default,_result,,,
,result,table,message,value
,,1,howdy,5
,,1,hello there,5
,,1,hi,6
`.trim()

    const expectedData = [
      ['#group', 'false', 'false', 'false', 'false'],
      ['#datatype', 'string', 'long', 'string', 'long'],
      ['#default', '_result', '', '', ''],
      ['', 'result', 'table', 'message', 'value'],
      ['', '', '0', 'howdy', '5'],
      ['', '', '0', 'hello there', '5'],
      ['', '', '0', 'hi', '6'],
      [],
      ['#group', 'false', 'false', 'false', 'false'],
      ['#datatype', 'string', 'long', 'string', 'long'],
      ['#default', '_result', '', '', ''],
      ['', 'result', 'table', 'message', 'value'],
      ['', '', '1', 'howdy', '5'],
      ['', '', '1', 'hello there', '5'],
      ['', '', '1', 'hi', '6'],
    ]

    const expected = {
      data: expectedData,
      maxColumnCount: 5,
    }

    expect(parseFiles([CSV])).toEqual(expected)
  })

  test('can parse multi-csv response with values containing newlines', () => {
    const CSV = `
#group,false,false,false,false
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
,,1,hi,6
`.trim()

    const expectedData = [
      ['#group', 'false', 'false', 'false', 'false'],
      ['#datatype', 'string', 'long', 'string', 'long'],
      ['#default', '_result', '', '', ''],
      ['', 'result', 'table', 'message', 'value'],
      ['', '', '0', 'howdy', '5'],
      ['', '', '0', 'hello\n\nthere', '5'],
      ['', '', '0', 'hi', '6'],
      [],
      ['#group', 'false', 'false', 'false', 'false'],
      ['#datatype', 'string', 'long', 'string', 'long'],
      ['#default', '_result', '', '', ''],
      ['', 'result', 'table', 'message', 'value'],
      ['', '', '1', 'howdy', '5'],
      ['', '', '1', 'hello\n\nthere', '5'],
      ['', '', '1', 'hi', '6'],
    ]

    const expected = {
      data: expectedData,
      maxColumnCount: 5,
    }

    expect(parseFiles([CSV])).toEqual(expected)
  })
})
