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

const validRelativeTimeRange = (timeRange: TimeRange): TimeRange | null => {
  const matchedRange = timeRanges.find(t => t.lower === timeRange.lower)

  if (matchedRange) {
    return matchedRange
  }

  return null
}

export const validAbsoluteTimeRange = (
  timeRange: TimeRange
): TimeRange | null => {
  if (
    timeRange.lower &&
    timeRange.upper &&
    moment(timeRange.lower).isValid() &&
    moment(timeRange.upper).isValid() &&
    moment(timeRange.lower).isBefore(moment(timeRange.upper))
  ) {
    return timeRange
  }
  return null
}

export const validTimeRange = (timeRange: TimeRange): TimeRange | null => {
  let timeRangeOrNull = validRelativeTimeRange(timeRange)
  if (!timeRangeOrNull) {
    timeRangeOrNull = validAbsoluteTimeRange(timeRange)
  }
  return timeRangeOrNull
}
