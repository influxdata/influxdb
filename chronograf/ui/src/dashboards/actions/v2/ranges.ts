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
import {defaultTimeRange} from 'src/shared/data/timeRanges'

// Types
import {TimeRange} from 'src/types'

export type Action =
  | SetDashTimeV1Action
  | SetZoomedTimeRangeAction
  | DeleteTimeRangeAction
  | RetainRangesDashTimeV1Action

export enum ActionTypes {
  DeleteTimeRange = 'DELETE_TIME_RANGE',
  SetTimeRange = 'SET_DASHBOARD_TIME_RANGE',
  SetDashboardTimeV1 = 'SET_DASHBOARD_TIME_V1',
  RetainRangesDashboardTimeV1 = 'RETAIN_RANGES_DASHBOARD_TIME_V1',
  SetZoomedTimeRange = 'SET_DASHBOARD_ZOOMED_TIME_RANGE',
}

interface DeleteTimeRangeAction {
  type: ActionTypes.DeleteTimeRange
  payload: {
    dashboardID: string
  }
}

interface SetDashTimeV1Action {
  type: ActionTypes.SetDashboardTimeV1
  payload: {
    dashboardID: string
    timeRange: TimeRange
  }
}

interface SetZoomedTimeRangeAction {
  type: ActionTypes.SetZoomedTimeRange
  payload: {
    zoomedTimeRange: TimeRange
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

export const setZoomedTimeRange = (
  zoomedTimeRange: TimeRange
): SetZoomedTimeRangeAction => ({
  type: ActionTypes.SetZoomedTimeRange,
  payload: {zoomedTimeRange},
})

export const setDashTimeV1 = (
  dashboardID: string,
  timeRange: TimeRange
): SetDashTimeV1Action => ({
  type: ActionTypes.SetDashboardTimeV1,
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

  const zoomedTimeRangeFromQueries = {
    lower: queryParams.zoomedLower,
    upper: queryParams.zoomedUpper,
  }

  let validatedTimeRange = validTimeRange(timeRangeFromQueries)

  if (!validatedTimeRange.lower) {
    const dashboardTimeRange = ranges.find(r => r.dashboardID === dashboardID)

    validatedTimeRange = dashboardTimeRange || defaultTimeRange

    if (timeRangeFromQueries.lower || timeRangeFromQueries.upper) {
      dispatch(notify(copy.invalidTimeRangeValueInURLQuery()))
    }
  }

  dispatch(setDashTimeV1(dashboardID, validatedTimeRange))

  const validatedZoomedTimeRange = validAbsoluteTimeRange(
    zoomedTimeRangeFromQueries
  )

  if (
    !validatedZoomedTimeRange.lower &&
    (queryParams.zoomedLower || queryParams.zoomedUpper)
  ) {
    dispatch(notify(copy.invalidZoomedTimeRangeValueInURLQuery()))
  }

  dispatch(setZoomedTimeRange(validatedZoomedTimeRange))

  const updatedQueryParams = {
    lower: validatedTimeRange.lower,
    upper: validatedTimeRange.upper,
    zoomedLower: validatedZoomedTimeRange.lower,
    zoomedUpper: validatedZoomedTimeRange.upper,
  }

  dispatch(updateQueryParams(updatedQueryParams))
}
