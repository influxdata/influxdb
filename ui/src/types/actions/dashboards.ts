import {Dispatch} from 'redux'
import {InjectedRouter} from 'react-router'
import {LocationAction} from 'react-router-redux'
import {Source} from 'src/types'
import * as DashboardData from 'src/types/dashboard'
import * as QueryData from 'src/types/query'
import * as TempVarData from 'src/types/tempVars'
import * as ErrorActions from 'src/types/actions/error'
import * as NotificationActions from 'src/shared/actions/notifications'
import * as DashboardReducers from 'src/types/reducers/dashboards'
import {Location} from 'history'

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

export type LoadDashboardActionCreator = (
  dashboard: DashboardData.Dashboard
) => LoadDashboardAction

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

export type SetDashTimeV1ActionCreator = (
  dashboardID: number,
  timeRange: QueryData.TimeRange
) => SetDashTimeV1Action

export interface RetainRangesDashTimeV1Action {
  type: 'RETAIN_RANGES_DASHBOARD_TIME_V1'
  payload: {
    dashboardIDs: string[]
  }
}

export type RetainRangesDashTimeV1ActionCreator = (
  dashboardIDs: string[]
) => RetainRangesDashTimeV1Action

export type SetTimeRangeActionCreator = (
  timeRange: QueryData.TimeRange
) => SetTimeRangeAction

export interface SetTimeRangeAction {
  type: 'SET_DASHBOARD_TIME_RANGE'
  payload: {
    timeRange: QueryData.TimeRange
  }
}

export type SetZoomedTimeRangeDispatcher = (
  zoomedTimeRange: QueryData.TimeRange,
  location: Location
) => SetZoomedTimeRangeThunk

export type SetZoomedTimeRangeThunk = (
  dispatch: Dispatch<
    | SetZoomedTimeRangeActionCreator
    | SyncURLQueryFromQueryParamsObjectDispatcher
  >
) => Promise<void>

export type SetZoomedTimeRangeActionCreator = (
  zoomedTimeRange: QueryData.TimeRange
) => SetZoomedTimeRangeAction

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

export type UpdateDashboardActionCreator = (
  dashboard: DashboardData.Dashboard
) => UpdateDashboardAction

export type CreateDashboardActionCreator = (
  dashboard: DashboardData.Dashboard
) => CreateDashboardAction

export interface CreateDashboardAction {
  type: 'CREATE_DASHBOARD'
  payload: {
    dashboard: DashboardData.Dashboard
  }
}

export type DeleteDashboardActionCreator = (
  dashboard: DashboardData.Dashboard
) => DeleteDashboardAction

export interface DeleteDashboardAction {
  type: 'DELETE_DASHBOARD'
  payload: {
    dashboard: DashboardData.Dashboard
  }
}

export type DeleteDashboardFailedActionCreator = (
  dashboard: DashboardData.Dashboard
) => DeleteDashboardFailedAction

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

export type AddDashboardCellDispatcher = (
  dashboard: DashboardData.Dashboard,
  cellType?: DashboardData.CellType
) => AddDashboardCellThunk

export type AddDashboardCellThunk = (
  dispatch: Dispatch<
    | AddDashboardCellAction
    | NotificationActions.PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
) => Promise<void>

export type AddDashboardCellActionCreator = (
  dashboard: DashboardData.Dashboard,
  cell: DashboardData.Cell
) => AddDashboardCellAction

export interface AddDashboardCellAction {
  type: 'ADD_DASHBOARD_CELL'
  payload: {
    dashboard: DashboardData.Dashboard
    cell: DashboardData.Cell
  }
}

export type CloneDashboardCellDispatcher = (
  dashboard: DashboardData.Dashboard,
  cell: DashboardData.Cell
) => CloneDashboardCellThunk

export type CloneDashboardCellThunk = (
  dispatch: Dispatch<
    | AddDashboardCellAction
    | NotificationActions.PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
) => Promise<void>

export type DeleteDashboardCellDispatcher = (
  dashboard: DashboardData.Dashboard,
  cell: DashboardData.Cell
) => DeleteDashboardCellThunk

export type DeleteDashboardCellThunk = (
  dispatch: Dispatch<
    | DeleteDashboardCellActionCreator
    | NotificationActions.PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
) => Promise<void>

export type DeleteDashboardCellActionCreator = (
  dashboard: DashboardData.Dashboard,
  cell: DashboardData.Cell
) => DeleteDashboardCellAction

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

export type TemplateVariableSelectedActionCreator = (
  dashboardID: number,
  templateID: string,
  values: any[]
) => TemplateVariableSelectedAction

export interface TemplateVariableSelectedAction {
  type: 'TEMPLATE_VARIABLE_SELECTED'
  payload: {
    dashboardID: number
    templateID: string
    values: any[]
  }
}

export type TemplateVariablesSelectedByNameActionCreator = (
  dashboardID: number,
  queryParams: TempVarData.URLQueryParams
) => TemplateVariablesSelectedByNameAction

export interface TemplateVariablesSelectedByNameAction {
  type: 'TEMPLATE_VARIABLES_SELECTED_BY_NAME'
  payload: {
    dashboardID: number
    queryParams: TempVarData.URLQueryParams
  }
}

export type EditTemplateVariableValuesActionCreator = (
  dashboardID: number,
  templateID: string,
  values: any[]
) => EditTemplateVariableValuesAction

export interface EditTemplateVariableValuesAction {
  type: 'EDIT_TEMPLATE_VARIABLE_VALUES'
  payload: {
    dashboardID: number
    templateID: string
    values: any[]
  }
}

export type SetHoverTimeActionCreator = (
  hoverTime: string
) => SetHoverTimeAction

export interface SetHoverTimeAction {
  type: 'SET_HOVER_TIME'
  payload: {
    hoverTime: string
  }
}

export type SetActiveCellActionCreator = (
  activeCellID: string
) => SetActiveCellAction

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

export type PutDashboardByIDDispatcher = (
  dashboardID: number
) => PutDashboardByIDThunk

export type PutDashboardByIDThunk = (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>,
  getState: () => DashboardReducers.Dashboards
) => Promise<void>

export type DeleteDashboardDispatcher = (
  dashboard: DashboardData.Dashboard
) => DeleteDashboardThunk

export type DeleteDashboardThunk = (
  dispatch: Dispatch<
    | DeleteDashboardActionCreator
    | NotificationActions.PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
    | DeleteDashboardFailedActionCreator
  >
) => Promise<void>

export type UpdateDashboardCellDispatcher = (
  dashboard: DashboardData.Dashboard,
  cell: DashboardData.Cell
) => UpdateDashboardCellThunk

export type UpdateDashboardCellThunk = (
  dispatch: Dispatch<
    SyncDashboardCellActionCreator | ErrorActions.ErrorThrownActionCreator
  >
) => Promise<void>

export type SyncURLQueryFromQueryParamsObjectDispatcher = (
  location: Location,
  updatedURLQueryParams: TempVarData.URLQueryParams,
  deletedURLQueryParams?: TempVarData.URLQueryParams
) => SyncURLQueryFromQueryParamsObjectActionCreator

export type SyncURLQueryFromTempVarsDispatcher = (
  location: Location,
  tempVars: TempVarData.Template[],
  deletedTempVars: TempVarData.Template[],
  urlQueryParamsTimeRanges?: TempVarData.URLQueryParams
) => SyncURLQueryFromQueryParamsObjectActionCreator

export type SyncURLQueryFromQueryParamsObjectActionCreator = (
  dispatch: Dispatch<LocationAction>
) => void

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
