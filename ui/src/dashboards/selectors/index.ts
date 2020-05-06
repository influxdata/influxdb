import {get} from 'lodash'

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
