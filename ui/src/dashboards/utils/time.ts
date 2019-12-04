import {CustomTimeRange, TimeRange, DurationTimeRange} from 'src/types/queries'
import {isNull} from 'lodash'
import moment from 'moment'

import {
  SELECTABLE_TIME_RANGES,
  TIME_RANGE_FORMAT,
} from 'src/shared/constants/timeRanges'
import {isDateParseable} from 'src/variables/utils/getTimeRangeVars'
import {isDurationParseable} from 'src/shared/utils/duration'

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

export const validateAndTypeRange = (timeRange: {
  lower: string
  upper: string
}): TimeRange => {
  const {lower, upper} = timeRange
  if (isDateParseable(lower) && isDateParseable(upper)) {
    const label = `${moment(timeRange.lower).format(
      TIME_RANGE_FORMAT
    )} - ${moment(timeRange.upper).format(TIME_RANGE_FORMAT)}`

    return {
      ...timeRange,
      type: 'custom',
      label,
    } as CustomTimeRange
  }
  if (isDurationParseable(lower) && isNull(upper)) {
    const selectedTimeRange = SELECTABLE_TIME_RANGES.find(r => {
      r.lower === lower
    })

    if (selectedTimeRange) {
      return selectedTimeRange
    }

    return {
      ...timeRange,
      type: 'duration',
      label: timeRange.lower,
    } as DurationTimeRange
  }
  return null
}
