import getRange from 'src/shared/parsing/getRangeForDygraph'

const date = new Date()
const max = 20
const mid = 10
const min = 5
const negMax = -20

describe('getRangeForDygraphSpec', () => {
  it('gets the range for one timeSeries', () => {
    const timeSeries = [[date, min], [date, mid], [date, max]]
    const actual = getRange(timeSeries)
    const expected = [min, max]

    expect(actual).toEqual(expected)
  })

  it('does not get range when a range is provided', () => {
    const timeSeries = [[date, min], [date, max], [date, mid]]
    const providedRange = ['0', '4']
    const actual = getRange(timeSeries, providedRange)

    expect(actual).toEqual([0, 4])
  })

  it('gets the range for multiple timeSeries', () => {
    const timeSeries = [[date, null, min], [date, max, mid], [date, null, mid]]
    const actual = getRange(timeSeries)
    const expected = [min, max]

    expect(actual).toEqual(expected)
  })

  describe('if min and max are equal', () => {
    it('it sets min to 0 if they are positive', () => {
      const timeSeries = [[date, max], [date, max], [date, max]]
      const actual = getRange(timeSeries)
      const expected = [0, max]

      expect(actual).toEqual(expected)
    })

    it('it sets max to 0 if they are negative', () => {
      const timeSeries = [[date, negMax], [date, negMax], [date, negMax]]
      const actual = getRange(timeSeries)
      const expected = [negMax, 0]

      expect(actual).toEqual(expected)
    })
  })
})
