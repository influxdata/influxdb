import moment from 'moment'
import {
  INFLUXQL,
  ABSOLUTE,
  INVALID,
  RELATIVE_LOWER,
  RELATIVE_UPPER,
} from 'shared/constants/timeRange'
const now = /^now/

export const timeRangeType = ({upper, lower, type}) => {
  if (!upper && !lower) {
    return INVALID
  }

  if (type && type !== INFLUXQL) {
    return INVALID
  }

  const isUpperValid = moment(upper).isValid()
  const isLowerValid = moment(lower).isValid()

  // {lower: <Date>, upper: <Date>}
  if (isLowerValid && isUpperValid) {
    return ABSOLUTE
  }

  // {lower: now - <Duration>, upper: <empty>}
  if (now.test(lower) && !upper) {
    return RELATIVE_LOWER
  }

  // {lower: <Date>, upper: now() - <Duration>}
  if (isLowerValid && now.test(upper)) {
    return RELATIVE_UPPER
  }

  return INVALID
}

// based on time range type, calc the time shifted dates
