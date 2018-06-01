import {TimeRange} from 'src/types/query'
import moment from 'moment'

import {timeRanges} from 'src/shared/data/timeRanges'

interface InputTimeRange {
  seconds?: number
  lower?: string
  upper?: string
}

interface OutputTimeRange {
  since: number
  until: number | null
}

export const millisecondTimeRange = ({
  seconds,
  lower,
  upper,
}: InputTimeRange): OutputTimeRange => {
  // Is this a relative time range?
  if (seconds) {
    return {since: Date.now() - seconds * 1000, until: null}
  }

  const since = Date.parse(lower)
  let until
  if (upper === 'now()') {
    until = Date.now()
  } else {
    until = Date.parse(upper)
  }
  return {since, until}
}

export const isRelativeTimeRange = timeRange =>
  timeRange.lower.includes('now()')

export const validTimeRange = (timeRange: TimeRange): boolean => {
  if (!timeRange || !timeRange.lower) {
    return false
  }

  const isValidRelativeTimeRange = !!timeRanges.find(
    t => t.lower === timeRange.lower
  )

  const isUpperValid = timeRange.upper && moment(timeRange.upper).isValid()
  const isLowerValid = moment(timeRange.lower).isValid()

  if (!isTimeValid) {
    return false
  }
  return true
}
