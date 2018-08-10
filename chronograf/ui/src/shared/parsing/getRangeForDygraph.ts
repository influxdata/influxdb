const checkNumeric = num => (isFinite(num) ? num : null)

const considerEmpty = (userNumber, num) => {
  if (userNumber) {
    return +userNumber
  }

  return num
}

const getRange = (
  timeSeries,
  userSelectedRange = [null, null]
): [number, number] => {
  const [uMin, uMax] = userSelectedRange
  const userMin = checkNumeric(uMin)
  const userMax = checkNumeric(uMax)

  const points = [...timeSeries, [null, uMax], [null, uMax]]

  const range = points.reduce(
    // tslint:disable-next-line
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

  // prevents inversion of graph
  if (min > max) {
    return [min, min + 1]
  }

  return [min, max]
}

const coerceToNum = str => (str ? +str : null)
export const getStackedRange = (bounds = [null, null]): [number, number] => [
  coerceToNum(bounds[0]),
  coerceToNum(bounds[1]),
]

export default getRange
