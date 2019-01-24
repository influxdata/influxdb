import {TimeRange} from 'src/types/v2'
import {TIME_RANGE_START} from 'src/shared/constants'

export const timeRangeVariables = (
  timeRange: TimeRange
): {[key: string]: string} => {
  const result: {[key: string]: string} = {}

  result[TIME_RANGE_START] = timeRange.lower
    .replace('now()', '')
    .replace(/\s/g, '')

  return result
}
