import parseValuesColumn from 'src/shared/parsing/flux/values'
import {TAGS_RESPONSE} from 'test/shared/parsing/flux/constants'

describe('tagKeys parser', () => {
  it('returns no measurements for an empty results response', () => {
    expect(parseValuesColumn('')).toEqual([])
  })

  it('returns the approriate tagKeys', () => {
    const actual = parseValuesColumn(TAGS_RESPONSE)
    const expected = [
      '_field',
      '_measurement',
      'cpu',
      'device',
      'fstype',
      'host',
      'mode',
      'name',
      'path',
    ]

    expect(actual).toEqual(expected)
  })
})
