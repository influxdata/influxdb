import {Dispatch} from 'redux'
import {InjectedRouter} from 'react-router'
import {LocationAction} from 'react-router-redux'
import {Source} from 'src/types'
import * as DashboardData from 'src/types/dashboard'
import * as QueryData from 'src/types/query'
import * as TempVarData from 'src/types/tempVars'
import * as ErrorActions from 'src/shared/actions/errors'
import * as NotificationActions from 'src/shared/actions/notifications'
import * as DashboardReducers from 'src/types/reducers/dashboard'

export type LoadDashboardsActionCreator = (
  dashboards: DashboardData.Dashboard[],
  dashboardID?: number
) => LoadDashboardsAction

export interface LoadDashboardsAction {
  type: 'LOAD_DASHBOARDS'
  payload: {
    dashboards: DashboardData.Dashboard[]
    dashboardID: number
  }
}

export interface LoadDashboardAction {
  type: 'LOAD_DASHBOARD'
  payload: {
    dashboard: DashboardData.Dashboard
  }
}

export interface SetDashTimeV1Action {
  type: 'SET_DASHBOARD_TIME_V1'
  payload: {
    dashboardID: number
    timeRange: QueryData.TimeRange
  }
}

export interface RetainRangesDashTimeV1Action {
  type: 'RETAIN_RANGES_DASHBOARD_TIME_V1'
  payload: {
    dashboardIDs: string[]
  }
}

export type SetTimeRangeActionCreator = (
  timeRange: QueryData.TimeRange
) => SetTimeRangeAction

export interface SetTimeRangeAction {
  type: 'SET_DASHBOARD_TIME_RANGE'
  payload: {
    timeRange: QueryData.TimeRange
  }
}

export interface SetZoomedTimeRangeAction {
  type: 'SET_DASHBOARD_ZOOMED_TIME_RANGE'
  payload: {
    zoomedTimeRange: QueryData.TimeRange
  }
}

export interface UpdateDashboardAction {
  type: 'UPDATE_DASHBOARD'
  payload: {
    dashboard: DashboardData.Dashboard
  }
}

export interface CreateDashboardAction {
  type: 'CREATE_DASHBOARD'
  payload: {
    dashboard: DashboardData.Dashboard
  }
}

export interface DeleteDashboardAction {
  type: 'DELETE_DASHBOARD'
  payload: {
    dashboard: DashboardData.Dashboard
    dashboardID: number
  }
}

export interface DeleteDashboardFailedAction {
  type: 'DELETE_DASHBOARD_FAILED'
  payload: {
    dashboard: DashboardData.Dashboard
  }
}

export type SyncDashboardCellActionCreator = (
  dashboard: DashboardData.Dashboard,
  cell: DashboardData.Cell
) => SyncDashboardCellAction

export interface SyncDashboardCellAction {
  type: 'SYNC_DASHBOARD_CELL'
  payload: {
    dashboard: DashboardData.Dashboard
    cell: DashboardData.Cell
  }
}

export interface AddDashboardCellAction {
  type: 'ADD_DASHBOARD_CELL'
  payload: {
    dashboard: DashboardData.Dashboard
    cell: DashboardData.Cell
  }
}
export interface DeleteDashboardCellAction {
  type: 'DELETE_DASHBOARD_CELL'
  payload: {
    dashboard: DashboardData.Dashboard
    cell: DashboardData.Cell
  }
}

export type EditCellQueryStatusActionCreator = (
  queryID: string,
  status: string
) => EditCellQueryStatusAction

export interface EditCellQueryStatusAction {
  type: 'EDIT_CELL_QUERY_STATUS'
  payload: {
    queryID: string
    status: string
  }
}

export interface TemplateVariableSelectedAction {
  type: 'TEMPLATE_VARIABLE_SELECTED'
  payload: {
    dashboardID: number
    templateID: string
    values: any[]
  }
}

export interface TemplateVariablesSelectedByNameAction {
  type: 'TEMPLATE_VARIABLES_SELECTED_BY_NAME'
  payload: {
    dashboardID: number
    queryParams: TempVarData.URLQueryParams
  }
}

export interface EditTemplateVariableValuesAction {
  type: 'EDIT_TEMPLATE_VARIABLE_VALUES'
  payload: {
    dashboardID: number
    templateID: string
    values: any[]
  }
}

export interface SetHoverTimeAction {
  type: 'SET_HOVER_TIME'
  payload: {
    hoverTime: string
  }
}

export interface SetActiveCellAction {
  type: 'SET_ACTIVE_CELL'
  payload: {
    activeCellID: string
  }
}

export type GetDashboardsDispatcher = () => GetDashboardsThunk

export type GetDashboardsThunk = (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>
) => Promise<DashboardData.Dashboard[] | void>

export type GetDashboardsNamesDispatcher = (
  sourceID: string
) => GetDashboardsNamesThunk

export type GetDashboardsNamesThunk = (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>
) => Promise<DashboardData.DashboardName[] | void>

export type PutDashboardDispatcher = (
  dashboard: DashboardData.Dashboard
) => PutDashboardThunk

export type PutDashboardThunk = (
  dispatch: Dispatch<
    UpdateDashboardAction | ErrorActions.ErrorThrownActionCreator
  >
) => Promise<void>

export type PutDashboardByIDThunk = (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>,
  getState: () => DashboardReducers.Dashboards
) => Promise<void>

export type PutDashboardByIDDispatcher = (
  dashboardID: number
) => PutDashboardByIDThunk

export type UpdateDashboardCellDispatcher = (
  dashboard: DashboardData.Dashboard,
  cell: DashboardData.Cell
) => UpdateDashboardCellThunk

export type UpdateDashboardCellThunk = (
  dispatch: Dispatch<
    SyncDashboardCellActionCreator | ErrorActions.ErrorThrownActionCreator
  >
) => Promise<void>

export type AddDashboardCellDispatcher = (
  dashboard: DashboardData.Dashboard,
  cellType: DashboardData.CellType
) => AddDashboardCellThunk

export type AddDashboardCellThunk = (
  dispatch: Dispatch<
    | AddDashboardCellAction
    | NotificationActions.PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
) => Promise<void>

export type SyncURLQueryFromQueryParamsObjectDispatcher = (
  location: Location,
  updatedURLQueryParams: TempVarData.URLQueryParams,
  deletedURLQueryParams: TempVarData.URLQueryParams
) => SyncURLQueryFromQueryParamsObjectActionCreator

export type SyncURLQueryFromQueryParamsObjectActionCreator = (
  dispatch: Dispatch<LocationAction>
) => void

export type SyncURLQueryFromTempVarsDispatcher = (
  location: Location,
  tempVars: TempVarData.Template[],
  deletedTempVars: TempVarData.Template[],
  urlQueryParamsTimeRanges: TempVarData.URLQueryParams
) => SyncURLQueryFromQueryParamsObjectActionCreator

export type SyncDashboardTempVarsFromURLQueryParamsDispatcher = (
  dispatch: Dispatch<
    | NotificationActions.PublishNotificationActionCreator
    | TemplateVariableSelectedAction
  >,
  getState: () => DashboardReducers.Dashboards & DashboardReducers.Auth
) => void

export type SyncDashboardTimeRangeFromURLQueryParamsDispatcher = (
  dispatch: Dispatch<NotificationActions.PublishNotificationActionCreator>,
  getState: () => DashboardReducers.Dashboards & DashboardReducers.DashTimeV1
) => void

export type SyncDashboardFromURLQueryParamsDispatcher = (
  dispatch: Dispatch<
    | SyncDashboardTempVarsFromURLQueryParamsDispatcher
    | SyncDashboardTimeRangeFromURLQueryParamsDispatcher
  >
) => void

export type GetDashboardWithHydratedAndSyncedTempVarsAsyncDispatcher = (
  dashboardID: number,
  source: Source,
  router: InjectedRouter,
  location: Location
) => GetDashboardWithHydratedAndSyncedTempVarsAsyncThunk

export type GetDashboardWithHydratedAndSyncedTempVarsAsyncThunk = (
  dispatch: Dispatch<NotificationActions.PublishNotificationActionCreator>
) => Promise<void>
