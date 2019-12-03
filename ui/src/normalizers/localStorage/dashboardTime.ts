// Libraries
import {isString, isNull, isObject} from 'lodash'

// Types
import {RangeState} from 'src/dashboards/reducers/ranges'
import {CUSTOM_TIME_RANGE_LABEL} from 'src/types'

const isCorrectType = (bound: any) => isString(bound) || isNull(bound)
const isKnownType = (type: any) => isString(type) && true // TODO check that type is one of the known ones.

export const getLocalStateRangesAsArray = (ranges: any[]): RangeState => {
  const normalizedRanges = ranges.filter(r => {
    if (!isObject(r)) {
      return false
    }

    // check for presence of keys
    if (
      !r.hasOwnProperty('dashboardID') ||
      !r.hasOwnProperty('lower') ||
      !r.hasOwnProperty('upper')
    ) {
      return false
    }

    const {dashboardID, lower, upper} = r

    if (!dashboardID || typeof dashboardID !== 'string') {
      return false
    }

    if (!lower && !upper) {
      return false
    }

    if (!isCorrectType(lower) || !isCorrectType(upper)) {
      return false
    }

    return true
  })

  const rangesObject: RangeState = {}

  normalizedRanges.forEach(
    (range: {dashboardID: string; lower: string; upper: string}) => {
      const {dashboardID, ...rest} = range
      // TODO assign types here
      rangesObject[dashboardID] = {
        ...rest,
        type: 'custom',
        label: 'Custom Time Range' as CUSTOM_TIME_RANGE_LABEL,
      }
    }
  )

  return rangesObject
}

const normalizeRangesState = (ranges: RangeState): RangeState => {
  const normalized = {}

  for (const key in ranges) {
    if (
      isObject(ranges[key]) &&
      ranges[key].hasOwnProperty('upper') &&
      ranges[key].hasOwnProperty('lower') &&
      ranges[key].hasOwnProperty('type') &&
      isCorrectType(ranges[key].lower) &&
      isCorrectType(ranges[key].upper) &&
      isKnownType(ranges[key]['type'])
    ) {
      //TODO further validate based on type
      normalized[key] = ranges[key]
    }
  }

  return normalized
}

export const getLocalStateRanges = (ranges: RangeState | any[]) => {
  if (Array.isArray(ranges)) {
    return getLocalStateRangesAsArray(ranges)
  } else if (isObject(ranges)) {
    return normalizeRangesState(ranges)
  } else {
    return {}
  }
}

export const setLocalStateRanges = (ranges: RangeState) => {
  return normalizeRangesState(ranges)
}
