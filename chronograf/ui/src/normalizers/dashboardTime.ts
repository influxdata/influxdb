import _ from 'lodash'
import {Range} from 'src/dashboards/reducers/v2/ranges'

const dashtime = (ranges: Range[]): Range[] => {
  if (!Array.isArray(ranges)) {
    return []
  }

  const normalized = ranges.filter(r => {
    if (!_.isObject(r)) {
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
      _.isString(bound) || _.isNull(bound) || _.isInteger(bound)

    if (!isCorrectType(lower) || !isCorrectType(upper)) {
      return false
    }

    return true
  })

  return normalized
}

export default dashtime
