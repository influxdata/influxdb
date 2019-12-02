// Libraries
import qs from 'qs'
import {replace, RouterAction} from 'react-router-redux'
import {Dispatch, Action} from 'redux'
import _ from 'lodash'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Utils
import {validTimeRange, validAbsoluteTimeRange} from 'src/dashboards/utils/time'
import {stripPrefix} from 'src/utils/basepath'

// Constants
import * as copy from 'src/shared/copy/notifications'
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'

// Types
import {TimeRange} from 'src/types'

export type Action =
  | SetDashboardTimeRangeAction
  | DeleteTimeRangeAction
  | RetainRangesDashTimeV1Action

export enum ActionTypes {
  DeleteTimeRange = 'DELETE_TIME_RANGE',
  SetDashboardTimeRange = 'SET_DASHBOARD_TIME_RANGE',
  RetainRangesDashboardTimeV1 = 'RETAIN_RANGES_DASHBOARD_TIME_V1',
}

export interface DeleteTimeRangeAction {
  type: ActionTypes.DeleteTimeRange
  payload: {
    dashboardID: string
  }
}

interface SetDashboardTimeRangeAction {
  type: ActionTypes.SetDashboardTimeRange
  payload: {
    dashboardID: string
    timeRange: TimeRange
  }
}

interface RetainRangesDashTimeV1Action {
  type: ActionTypes.RetainRangesDashboardTimeV1
  payload: {
    dashboardIDs: string[]
  }
}

export const deleteTimeRange = (
  dashboardID: string
): DeleteTimeRangeAction => ({
  type: ActionTypes.DeleteTimeRange,
  payload: {dashboardID},
})

export const setDashboardTimeRange = (
  dashboardID: string,
  timeRange: TimeRange
): SetDashboardTimeRangeAction => ({
  type: ActionTypes.SetDashboardTimeRange,
  payload: {dashboardID, timeRange},
})

export const retainRangesDashTimeV1 = (
  dashboardIDs: string[]
): RetainRangesDashTimeV1Action => ({
  type: ActionTypes.RetainRangesDashboardTimeV1,
  payload: {dashboardIDs},
})

export const updateQueryParams = (updatedQueryParams: object): RouterAction => {
  const {search, pathname} = window.location
  const strippedPathname = stripPrefix(pathname)

  const newQueryParams = _.pickBy(
    {
      ...qs.parse(search, {ignoreQueryPrefix: true}),
      ...updatedQueryParams,
    },
    v => !!v
  )

  const newSearch = qs.stringify(newQueryParams)
  const newLocation = {pathname: strippedPathname, search: `?${newSearch}`}

  return replace(newLocation)
}

export const updateTimeRangeFromQueryParams = (dashboardID: string) => (
  dispatch: Dispatch<Action>,
  getState
): void => {
  const {ranges} = getState()
  const queryParams = qs.parse(window.location.search, {
    ignoreQueryPrefix: true,
  })

  const timeRangeFromQueries = {
    lower: queryParams.lower,
    upper: queryParams.upper,
  }


  let validatedTimeRange = validTimeRange(timeRangeFromQueries)

  if (!validatedTimeRange.lower) {
    const dashboardTimeRange = ranges.find(r => r.dashboardID === dashboardID)

    validatedTimeRange = dashboardTimeRange || DEFAULT_TIME_RANGE

    if (timeRangeFromQueries.lower || timeRangeFromQueries.upper) {
      dispatch(notify(copy.invalidTimeRangeValueInURLQuery()))
    }

  dispatch(setDashboardTimeRange(dashboardID, validatedTimeRange))

  const updatedQueryParams = {
    lower: validatedTimeRange.lower,
    upper: validatedTimeRange.upper,
  }

  dispatch(updateQueryParams(updatedQueryParams))
}
