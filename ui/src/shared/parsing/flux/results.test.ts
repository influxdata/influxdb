import {parseResponse} from 'src/shared/parsing/flux/response'
import {
  RESPONSE_NO_METADATA,
  RESPONSE_METADATA,
  MULTI_SCHEMA_RESPONSE,
  EXPECTED_COLUMNS,
  TRUNCATED_RESPONSE,
} from 'src/shared/parsing/flux/constants'

describe('Flux results parser', () => {
  it('parseResponse into the right number of tables', () => {
    const result = parseResponse(MULTI_SCHEMA_RESPONSE)
    expect(result).toHaveLength(4)
  })

  describe('headers', () => {
    it('throws when no metadata is present', () => {
      expect(() => {
        parseResponse(RESPONSE_NO_METADATA)
      }).toThrow()
    })

    it('can parse headers when metadata is present', () => {
      const actual = parseResponse(RESPONSE_METADATA)[0].data[0]
      expect(actual).toEqual(EXPECTED_COLUMNS)
    })
  })

  describe('group key', () => {
    it('parses the group key properly', () => {
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
    it('should discard tables without any non-annotation rows', () => {
      const actual = parseResponse(TRUNCATED_RESPONSE)

      expect(actual).toHaveLength(2)
    })
  })
})
