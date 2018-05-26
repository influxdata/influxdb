import parseTags from 'src/shared/parsing/v2/tags'
import {TAGS_RESPONSE} from 'test/shared/parsing/v2/constants'

describe('measurements parser', () => {
  it('returns no measurements for an empty results response', () => {
    expect(parseTags('')).toEqual([])
  })

  it('returns the approriate measurements', () => {
    const actual = parseTags(TAGS_RESPONSE)
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
