// Libraries
import {isObject, isString, isNull, isInteger} from 'lodash'

// Types
import {Range} from 'src/dashboards/reducers/ranges'

export const normalizeRanges = (ranges: Range[]): Range[] => {
  if (!Array.isArray(ranges)) {
    return []
  }

  const normalized = ranges.filter(r => {
    if (!isObject(r)) {
      return false
    }

    // check for presence of keys
    if (
      !r.hasOwnProperty('dashboardID') ||
      !r.hasOwnProperty('lower') ||
      !r.hasOwnProperty('upper')
    ) {
      return false
    }

    const {dashboardID, lower, upper} = r

    if (!dashboardID || typeof dashboardID !== 'string') {
      return false
    }

    if (!lower && !upper) {
      return false
    }

    const isCorrectType = bound =>
      isString(bound) || isNull(bound) || isInteger(bound)

    if (!isCorrectType(lower) || !isCorrectType(upper)) {
      return false
    }

    return true
  })

  return normalized
}
