import getRange from 'shared/parsing/getRangeForDygraph'

const date = new Date()
const max = 20
const mid = 10
const min = 5
const kapacitor = {value: null, rangeValue: null, operator: null}

describe.only('getRangeForDygraphSpec', () => {
  it('gets the range for one timeSeries', () => {
    const timeSeries = [[date, min], [date, mid], [date, max]]
    const actual = getRange(timeSeries)
    const expected = [min, max]

    expect(actual).to.deep.equal(expected)
  })

  it('does not get range when a range is provided', () => {
    const timeSeries = [[date, min], [date, max], [date, mid]]
    const providedRange = [0, 4]
    const actual = getRange(timeSeries, providedRange)

    expect(actual).to.deep.equal(providedRange)
  })

  it('gets the range for multiple timeSeries', () => {
    const timeSeries = [[date, null, min], [date, max, mid], [date, null, mid]]
    const actual = getRange(timeSeries)
    const expected = [min, max]

    expect(actual).to.deep.equal(expected)
  })

  it('returns a null array of two elements when min and max are equal', () => {
    const timeSeries = [[date, max], [date, max], [date, max]]
    const actual = getRange(timeSeries)
    const expected = [null, null]

    expect(actual).to.deep.equal(expected)
  })

  describe('when user provides a Kapacitor rule value', () => {
    const timeSeries = [[date, max], [date, mid], [date, min]]

    it('can pad positive values', () => {
      const [actualMin, actualMax] = getRange(timeSeries, undefined, {
        ...kapacitor,
        value: 20,
      })

      expect(actualMin).to.equal(min)
      expect(actualMax).to.be.above(max)
    })

    it('can pad negative values', () => {
      const [actualMin, actualMax] = getRange(timeSeries, undefined, {
        ...kapacitor,
        value: -10,
      })

      expect(actualMin).to.be.below(min)
      expect(actualMax).to.equal(max)
    })

    describe('when Kapacitor operator is "lower than" and value is outiside the range', () => {
      it('subtracts from a positive value', () => {
        const value = 2
        const opAndValue = {operator: 'less than', value}
        const [actualMin, actualMax] = getRange(timeSeries, undefined, {
          ...kapacitor,
          ...opAndValue,
        })

        expect(actualMin).to.be.lessThan(value)
        expect(actualMax).to.equal(max)
      })
    })
  })

  describe('when user provides a Kapacitor rule rangeValue', () => {
    const timeSeries = [[date, max], [date, min], [date, mid]]

    it('can pad positive values', () => {
      const [actualMin, actualMax] = getRange(timeSeries, undefined, {
        ...kapacitor,
        rangeValue: 20,
      })

      expect(actualMin).to.equal(min)
      expect(actualMax).to.be.above(max)
    })

    it('can pad negative values', () => {
      const [actualMin, actualMax] = getRange(timeSeries, undefined, {
        ...kapacitor,
        rangeValue: -10,
      })

      expect(actualMin).to.be.below(min)
      expect(actualMax).to.equal(max)
    })
  })
})
