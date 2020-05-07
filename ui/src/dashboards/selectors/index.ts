import {get} from 'lodash'
import moment from 'moment'
import {AppState, View, Check, ViewType, TimeRange, TimeZone} from 'src/types'
import {currentContext} from 'src/shared/selectors/currentContext'

// Constants
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'

export const getTimeRange = (state: AppState): TimeRange => {
  const contextID = currentContext(state)
  if (!state.ranges || !state.ranges.hasOwnProperty(contextID)) {
    return DEFAULT_TIME_RANGE
  }

  return state.ranges[contextID] || DEFAULT_TIME_RANGE
}

export const getTimeRangeWithTimezone = (state: AppState): TimeRange => {
  const timeRange = getTimeRange(state)
  const timeZone = getTimeZone(state)
  // create a copy of the timeRange so as not to mutate the original timeRange
  const newTimeRange = Object.assign({}, timeRange)
  if (timeRange.type === 'custom' && timeZone === 'UTC') {
    // check to see if the timeRange has an offset
    newTimeRange.lower = setTimeToUTC(newTimeRange.lower)
    newTimeRange.upper = setTimeToUTC(newTimeRange.upper)
  }
  return newTimeRange
}

export const setTimeToUTC = (date: string): string => {
  const offset = new Date(date).getTimezoneOffset()
  // check if date has offset
  if (offset === 0) {
    return date
  }
  let offsetDate = date
  if (offset > 0) {
    // subtract tz minute difference
    offsetDate = moment
      .utc(date)
      .subtract(offset, 'minutes')
      .format()
  }
  if (offset < 0) {
    // add tz minute difference
    offsetDate = moment
      .utc(date)
      .add(offset, 'minutes')
      .format()
  }
  return offsetDate
}

export const getTimeZone = (state: AppState): TimeZone => {
  return state.app.persisted.timeZone || 'Local'
}

export const getCheckForView = (
  state: AppState,
  view: View
): Partial<Check> => {
  const viewType: ViewType = get(view, 'properties.type')
  const checkID = get(view, 'properties.checkID')

  return viewType === 'check' ? state.resources.checks.byID[checkID] : null
}
