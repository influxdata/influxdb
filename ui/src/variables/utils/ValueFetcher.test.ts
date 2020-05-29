import {extractValues} from 'src/variables/utils/ValueFetcher'

const csv = `#group,false,false,false
#datatype,string,long,string
#default,_result,,
,result,table,_value
,,0,_field
,,0,_measurement
,,0,arch
,,0,build_date
,,0,commit`

describe('extractValues', () => {
  test('selects the first value if no preferences are provided', () => {
    const actual = extractValues(csv)

    const expected = {
      values: ['_field', '_measurement', 'arch', 'build_date', 'commit'],
      valueType: 'string',
      selected: ['_field'],
    }

    expect(actual).toEqual(expected)
  })

  test('selects the previous value if supplied', () => {
    const actual = extractValues(csv, 'arch', 'build_date')

    const expected = {
      values: ['_field', '_measurement', 'arch', 'build_date', 'commit'],
      valueType: 'string',
      selected: ['arch'],
    }

    expect(actual).toEqual(expected)
  })

  test('selects the default value if previous value doesnt exist', () => {
    const actual = extractValues(csv, 'howdy', 'build_date')

    const expected = {
      values: ['_field', '_measurement', 'arch', 'build_date', 'commit'],
      valueType: 'string',
      selected: ['build_date'],
    }

    expect(actual).toEqual(expected)
  })
})
