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

  const newTimeRange = {...timeRange}
  if (timeRange.type === 'custom' && timeZone === 'UTC') {
    // conforms dates to account to UTC with proper offset if needed
    newTimeRange.lower = setTimeToUTC(newTimeRange.lower)
    newTimeRange.upper = setTimeToUTC(newTimeRange.upper)
  }
  return newTimeRange
}

// The purpose of this function is to set a user's custom time range selection
// from the local time to the same time in UTC if UTC is selected from the
// timezone dropdown. This is feature was original requested here:
// https://github.com/influxdata/influxdb/issues/17877
// Example: user selected 10-11:00am and sets the dropdown to UTC
// Query should run against 10-11:00am UTC rather than querying
// 10-11:00am local time (offset depending on timezone)
export const setTimeToUTC = (date: string): string => {
  const offset = new Date(date).getTimezoneOffset()
  if (offset > 0) {
    return moment
      .utc(date)
      .subtract(offset, 'minutes')
      .format()
  }
  if (offset < 0) {
    return moment
      .utc(date)
      .add(offset, 'minutes')
      .format()
  }
  return moment.utc(date).format()
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
