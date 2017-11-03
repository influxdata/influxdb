import moment from 'moment'
import {ABSOLUTE} from 'shared/constants/timeRange'

// calc time range type
export const timeRangeType = ({upper, lower, type}) => {
  if (!upper && !lower) {
    return 'invalid'
  }

  if (!type || type === 'influxql') {
    const mUpper = moment(upper)
    const mLower = moment(lower)
    const isUpperValid = mUpper.isValid()
    const isLowerValid = mLower.isValid()

    if (isUpperValid && isLowerValid) {
      return ABSOLUTE
    }
  }

  return 'none'
}

// based on time range type, calc the time shifted dates
