import {parseResults} from 'src/shared/parsing/v2/results'
import {
  RESPONSE_NO_METADATA,
  RESPONSE_METADATA,
  RESPONSE_NO_MEASUREMENT,
  LARGE_RESPONSE,
  EXPECTED_METADATA,
  EXPECTED_COLUMNS,
} from 'test/shared/parsing/v2/constants'

describe('IFQL results parser', () => {
  it('parseResults into the right number of tables', () => {
    const result = parseResults(LARGE_RESPONSE)

    expect(result).toHaveLength(47)
  })

  describe('headers', () => {
    it('can parse headers when no metadata is present', () => {
      const actual = parseResults(RESPONSE_NO_METADATA)[0].data[0]

      expect(actual).toEqual(EXPECTED_COLUMNS)
    })

    it('can parse headers when metadata is present', () => {
      const actual = parseResults(RESPONSE_METADATA)[0].data[0]

      expect(actual).toEqual(EXPECTED_COLUMNS)
    })

    it('returns the approriate metadata', () => {
      const actual = parseResults(RESPONSE_METADATA)[0].metadata

      expect(actual).toEqual(EXPECTED_METADATA)
    })
  })

  describe('name', () => {
    it('uses the measurement as a name when present', () => {
      const actual = parseResults(RESPONSE_METADATA)[0].name
      const expected = 'cpu'

      expect(actual).toBe(expected)
    })

    it('uses the index as a name if a measurement column is not present', () => {
      const actual = parseResults(RESPONSE_NO_MEASUREMENT)[0].name
      const expected = 'Result 0'

      expect(actual).toBe(expected)
    })
  })
})
