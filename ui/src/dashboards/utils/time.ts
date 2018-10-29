import {TimeRange} from 'src/types/queries'
import moment from 'moment'

import {TIME_RANGES} from 'src/shared/constants/timeRanges'

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

const nullTimeRange: TimeRange = {lower: null, upper: null}

const validRelativeTimeRange = (timeRange: TimeRange): TimeRange => {
  const validatedTimeRange = TIME_RANGES.find(t => t.lower === timeRange.lower)

  if (validatedTimeRange) {
    return validatedTimeRange
  }

  return nullTimeRange
}

export const validAbsoluteTimeRange = (timeRange: TimeRange): TimeRange => {
  if (timeRange.lower && timeRange.upper) {
    if (moment(timeRange.lower).isValid()) {
      if (
        timeRange.upper === 'now()' ||
        (moment(timeRange.upper).isValid() &&
          moment(timeRange.lower).isBefore(moment(timeRange.upper)))
      ) {
        return timeRange
      }
    }
  }
  return nullTimeRange
}

export const validTimeRange = (timeRange: TimeRange): TimeRange => {
  const validatedTimeRange = validRelativeTimeRange(timeRange)
  if (validatedTimeRange.lower) {
    return validatedTimeRange
  }
  return validAbsoluteTimeRange(timeRange)
}
