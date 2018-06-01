import parseMeasurements from 'src/shared/parsing/v2/measurements'
import {MEASUREMENTS_RESPONSE} from 'test/shared/parsing/v2/constants'

describe('measurements parser', () => {
  it('returns no measurements for an empty results response', () => {
    expect(parseMeasurements('')).toEqual([])
  })

  it('returns the approriate measurements', () => {
    const actual = parseMeasurements(MEASUREMENTS_RESPONSE)
    const expected = ['disk', 'diskio']

    expect(actual).toEqual(expected)
  })
})
