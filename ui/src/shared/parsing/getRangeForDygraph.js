const PADDING_FACTOR = 0.1

const considerZero = (userNumber, number) => {
  if (userNumber === '') {
    return null
  }

  if (userNumber) {
    return +userNumber
  }

  return number
}

const getRange = (
  timeSeries,
  userSelectedRange = [null, null],
  ruleValues = {value: null, rangeValue: null}
) => {
  const {value, rangeValue, operator} = ruleValues
  const [userMin, userMax] = userSelectedRange

  if (userMin && userMax) {
    return [considerZero(userMin), considerZero(userMax)]
  }

  const subtractPadding = val => +val - Math.abs(val * PADDING_FACTOR)
  const addPadding = val => +val + Math.abs(val * PADDING_FACTOR)

  const pad = val => {
    if (val === null || val === '') {
      return null
    }

    if (operator === 'less than') {
      return val < 0 ? addPadding(val) : subtractPadding(val)
    }

    return val < 0 ? subtractPadding(val) : addPadding(val)
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

  // If time series is such that min and max are equal use Dygraph defaults
  if (range[0] === range[1]) {
    return [null, null]
  }

  const [min, max] = range

  return [considerZero(userMin, min), considerZero(userMax, max)]
}

export default getRange
