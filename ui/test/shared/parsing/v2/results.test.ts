import {parseResponse} from 'src/shared/parsing/v2/results'
import {
  RESPONSE_NO_METADATA,
  RESPONSE_METADATA,
  MULTI_SCHEMA_RESPONSE,
  EXPECTED_COLUMNS,
} from 'test/shared/parsing/v2/constants'

describe('IFQL results parser', () => {
  it('parseResponse into the right number of tables', () => {
    const result = parseResponse(MULTI_SCHEMA_RESPONSE)

    expect(result).toHaveLength(4)
  })

  describe('headers', () => {
    it('can parse headers when no metadata is present', () => {
      const actual = parseResponse(RESPONSE_NO_METADATA)[0].data[0]

      expect(actual).toEqual(EXPECTED_COLUMNS)
    })

    it('can parse headers when metadata is present', () => {
      const actual = parseResponse(RESPONSE_METADATA)[0].data[0]

      expect(actual).toEqual(EXPECTED_COLUMNS)
    })
  })

  describe('partition key', () => {
    it('parses the partition key propertly', () => {
      const actual = parseResponse(MULTI_SCHEMA_RESPONSE)[0].partitionKey
      const expected = {
        _field: 'usage_guest',
        _measurement: 'cpu',
        cpu: 'cpu-total',
        host: 'WattsInfluxDB',
      }

      expect(actual).toEqual(expected)
    })
  })
})
