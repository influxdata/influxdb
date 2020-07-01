// Libraries
import qs from 'qs'
import {replace, RouterAction} from 'connected-react-router'
import {Dispatch} from 'redux'
import {get, pickBy} from 'lodash'

// Actions
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'

// Utils
import {stripPrefix} from 'src/utils/basepath'
import {validateAndTypeRange} from 'src/dashboards/utils/time'

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

  const newQueryParams = pickBy(
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

export const updateQueryVars = varsObj => {
  const urlVars = qs.parse(window.location.search, {ignoreQueryPrefix: true})
  const vars = {
    ...(urlVars.vars || {}),
    ...varsObj,
  }

  return updateQueryParams({
    vars,
  })
}

export const updateTimeRangeFromQueryParams = (dashboardID: string) => (
  dispatch: Dispatch<Action | NotifyAction | RouterAction>,
  getState
): void => {
  const {ranges} = getState()
  const queryParams = qs.parse(window.location.search, {
    ignoreQueryPrefix: true,
  })

  const validatedTimeRangeFromQuery = validateAndTypeRange({
    lower: get(queryParams, 'lower', null),
    upper: get(queryParams, 'upper', null),
  })

  const validatedTimeRange =
    validatedTimeRangeFromQuery || ranges[dashboardID] || DEFAULT_TIME_RANGE

  if (
    (queryParams.lower || queryParams.upper) &&
    !validatedTimeRangeFromQuery
  ) {
    dispatch(notify(copy.invalidTimeRangeValueInURLQuery()))
  }

  dispatch(setDashboardTimeRange(dashboardID, validatedTimeRange))

  dispatch(
    updateQueryParams({
      lower: validatedTimeRange.lower,
      upper: validatedTimeRange.upper,
    })
  )
}
