import {CustomTimeRange, TimeRange, DurationTimeRange} from 'src/types/queries'

import {SELECTABLE_TIME_RANGES} from 'src/shared/constants/timeRanges'
import {isDateParseable} from 'src/variables/utils/getTimeRangeVars'
import {isDurationWithNowParseable} from 'src/shared/utils/duration'

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
    return {
      ...timeRange,
      type: 'custom',
    } as CustomTimeRange
  }

  if (isDurationWithNowParseable(lower)) {
    const selectableTimeRange = SELECTABLE_TIME_RANGES.find(
      r => r.lower === lower
    )

    if (selectableTimeRange) {
      return selectableTimeRange
    }

    return {
      lower,
      upper: null,
      type: 'duration',
    } as DurationTimeRange
  }
  return null
}
