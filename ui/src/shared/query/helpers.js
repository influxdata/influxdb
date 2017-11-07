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

export const shiftTimeRange = (timeRange, shift) => {
  const {upper, lower} = timeRange
  const {multiple, unit} = shift
  const trType = timeRangeType(timeRange)
  const duration = `${multiple}${unit}`
  const type = 'shifted'

  switch (trType) {
    case RELATIVE_UPPER:
    case ABSOLUTE: {
      return {
        lower: `${lower} - ${duration}`,
        upper: `${upper} - ${duration}`,
        type,
      }
    }

    case RELATIVE_LOWER: {
      return {
        lower: `${lower} - ${duration}`,
        upper: `now() - ${duration}`,
        type,
      }
    }

    default: {
      return {lower, upper, type: 'unshifted'}
    }
  }
}
