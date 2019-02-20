import {TimeRange} from 'src/types/v2'
import {TIME_RANGE_START, TIME_RANGE_STOP} from 'src/shared/constants'

export const timeRangeVariables = (
  timeRange: TimeRange
): {[key: string]: string} => {
  const result: {[key: string]: string} = {}

  result[TIME_RANGE_START] = timeRange.lower
    .replace('now()', '')
    .replace(/\s/g, '')

  if (timeRange.upper) {
    result[TIME_RANGE_STOP] = timeRange.upper
  } else {
    result[TIME_RANGE_STOP] = 'now()'
  }

  return result
}
