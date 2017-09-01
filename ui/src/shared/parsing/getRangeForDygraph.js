import BigNumber from 'bignumber.js'

const ADD_FACTOR = 1.1
const SUB_FACTOR = 0.9

const considerEmpty = (userNumber, number) => {
  if (userNumber) {
    return +userNumber
  }

  return number
}

const getRange = (
  timeSeries,
  userSelectedRange = [null, null],
  ruleValues = {value: null, rangeValue: null, operator: ''}
) => {
  const {value, rangeValue, operator} = ruleValues
  const [userMin, userMax] = userSelectedRange

  const addPad = bigNum => bigNum.times(ADD_FACTOR).toNumber()
  const subPad = bigNum => bigNum.times(SUB_FACTOR).toNumber()

  const pad = v => {
    if (v === null || v === '') {
      return null
    }

    const val = new BigNumber(v)

    if (operator === 'less than') {
      return val.lessThan(0) ? addPad(val) : subPad(val)
    }

    return val.lessThan(0) ? subPad(val) : addPad(val)
  }

  const points = [...timeSeries, [null, pad(value)], [null, pad(rangeValue)]]

  const range = points.reduce(
    ([min, max] = [], series) => {
      for (let i = 1; i < series.length; i++) {
        const val = series[i]

        if (max === null) {
          max = val
        }

        if (min === null) {
          min = val
        }

        if (typeof val === 'number') {
          min = Math.min(min, val)
          max = Math.max(max, val)
        }
      }

      return [min, max]
    },
    [null, null]
  )

  const [calcMin, calcMax] = range
  const min = considerEmpty(userMin, calcMin)
  const max = considerEmpty(userMax, calcMax)

  if (min === max) {
    if (min > 0) {
      return [0, max]
    }

    if (min < 0) {
      return [min, 0]
    }
  }

  return [min, max]
}

const parseNumber = bound => {
  if (bound) {
    return +bound
  }

  return null
}

export const getStackedRange = (bounds = [null, null]) => {
  const min = bounds[0]
  const max = bounds[1]

  return [parseNumber(min), parseNumber(max)]
}
export default getRange
