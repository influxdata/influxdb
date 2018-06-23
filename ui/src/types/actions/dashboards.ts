import {Dispatch} from 'redux'
import {InjectedRouter} from 'react-router'
import {LocationAction} from 'react-router-redux'
import {Source} from 'src/types'
import {Location} from 'history'
import * as Types from 'src/types/modules'

export type LoadDashboardsActionCreator = (
  dashboards: Types.Dashboards.Data.Dashboard[],
  dashboardID?: number
) => LoadDashboardsAction

export interface LoadDashboardsAction {
  type: 'LOAD_DASHBOARDS'
  payload: {
    dashboards: Types.Dashboards.Data.Dashboard[]
    dashboardID: number
  }
}

export type LoadDashboardActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
) => LoadDashboardAction

export interface LoadDashboardAction {
  type: 'LOAD_DASHBOARD'
  payload: {
    dashboard: Types.Dashboards.Data.Dashboard
  }
}

export interface SetDashTimeV1Action {
  type: 'SET_DASHBOARD_TIME_V1'
  payload: {
    dashboardID: number
    timeRange: Types.Queries.Data.TimeRange
  }
}

export type SetDashTimeV1ActionCreator = (
  dashboardID: number,
  timeRange: Types.Queries.Data.TimeRange
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
  timeRange: Types.Queries.Data.TimeRange
) => SetTimeRangeAction

export interface SetTimeRangeAction {
  type: 'SET_DASHBOARD_TIME_RANGE'
  payload: {
    timeRange: Types.Queries.Data.TimeRange
  }
}

export type SetZoomedTimeRangeDispatcher = (
  zoomedTimeRange: Types.Queries.Data.TimeRange,
  location: Location
) => SetZoomedTimeRangeThunk

export type SetZoomedTimeRangeThunk = (
  dispatch: Dispatch<
    | SetZoomedTimeRangeActionCreator
    | SyncURLQueryFromQueryParamsObjectDispatcher
  >
) => Promise<void>

export type SetZoomedTimeRangeActionCreator = (
  zoomedTimeRange: Types.Queries.Data.TimeRange
) => SetZoomedTimeRangeAction

export interface SetZoomedTimeRangeAction {
  type: 'SET_DASHBOARD_ZOOMED_TIME_RANGE'
  payload: {
    zoomedTimeRange: Types.Queries.Data.TimeRange
  }
}

export interface UpdateDashboardAction {
  type: 'UPDATE_DASHBOARD'
  payload: {
    dashboard: Types.Dashboards.Data.Dashboard
  }
}

export type UpdateDashboardActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
) => UpdateDashboardAction

export type CreateDashboardActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
) => CreateDashboardAction

export interface CreateDashboardAction {
  type: 'CREATE_DASHBOARD'
  payload: {
    dashboard: Types.Dashboards.Data.Dashboard
  }
}

export type DeleteDashboardActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
) => DeleteDashboardAction

export interface DeleteDashboardAction {
  type: 'DELETE_DASHBOARD'
  payload: {
    dashboard: Types.Dashboards.Data.Dashboard
  }
}

export type DeleteDashboardFailedActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
) => DeleteDashboardFailedAction

export interface DeleteDashboardFailedAction {
  type: 'DELETE_DASHBOARD_FAILED'
  payload: {
    dashboard: Types.Dashboards.Data.Dashboard
  }
}

export type SyncDashboardCellActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
) => SyncDashboardCellAction

export interface SyncDashboardCellAction {
  type: 'SYNC_DASHBOARD_CELL'
  payload: {
    dashboard: Types.Dashboards.Data.Dashboard
    cell: Types.Dashboards.Data.Cell
  }
}

export type AddDashboardCellDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cellType?: Types.Dashboards.Data.CellType
) => AddDashboardCellThunk

export type AddDashboardCellThunk = (
  dispatch: Dispatch<
    | AddDashboardCellAction
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
  >
) => Promise<void>

export type AddDashboardCellActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
) => AddDashboardCellAction

export interface AddDashboardCellAction {
  type: 'ADD_DASHBOARD_CELL'
  payload: {
    dashboard: Types.Dashboards.Data.Dashboard
    cell: Types.Dashboards.Data.Cell
  }
}

export type CloneDashboardCellDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
) => CloneDashboardCellThunk

export type CloneDashboardCellThunk = (
  dispatch: Dispatch<
    | AddDashboardCellAction
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
  >
) => Promise<void>

export type DeleteDashboardCellDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
) => DeleteDashboardCellThunk

export type DeleteDashboardCellThunk = (
  dispatch: Dispatch<
    | DeleteDashboardCellActionCreator
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
  >
) => Promise<void>

export type DeleteDashboardCellActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
) => DeleteDashboardCellAction

export interface DeleteDashboardCellAction {
  type: 'DELETE_DASHBOARD_CELL'
  payload: {
    dashboard: Types.Dashboards.Data.Dashboard
    cell: Types.Dashboards.Data.Cell
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
  queryParams: Types.TempVars.Data.URLQueryParams
) => TemplateVariablesSelectedByNameAction

export interface TemplateVariablesSelectedByNameAction {
  type: 'TEMPLATE_VARIABLES_SELECTED_BY_NAME'
  payload: {
    dashboardID: number
    queryParams: Types.TempVars.Data.URLQueryParams
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
  dispatch: Dispatch<Types.Errors.Actions.ErrorThrownActionCreator>
) => Promise<Types.Dashboards.Data.Dashboard[] | void>

export type GetDashboardsNamesDispatcher = (
  sourceID: string
) => GetDashboardsNamesThunk

export type GetDashboardsNamesThunk = (
  dispatch: Dispatch<Types.Errors.Actions.ErrorThrownActionCreator>
) => Promise<Types.Dashboards.Data.DashboardName[] | void>

export type PutDashboardDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard
) => PutDashboardThunk

export type PutDashboardThunk = (
  dispatch: Dispatch<
    UpdateDashboardAction | Types.Errors.Actions.ErrorThrownActionCreator
  >
) => Promise<void>

export type PutDashboardByIDDispatcher = (
  dashboardID: number
) => PutDashboardByIDThunk

export type PutDashboardByIDThunk = (
  dispatch: Dispatch<Types.Errors.Actions.ErrorThrownActionCreator>,
  getState: () => Types.Dashboards.Reducers.Dashboards
) => Promise<void>

export type DeleteDashboardDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard
) => DeleteDashboardThunk

export type DeleteDashboardThunk = (
  dispatch: Dispatch<
    | DeleteDashboardActionCreator
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
    | DeleteDashboardFailedActionCreator
  >
) => Promise<void>

export type UpdateDashboardCellDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
) => UpdateDashboardCellThunk

export type UpdateDashboardCellThunk = (
  dispatch: Dispatch<
    | SyncDashboardCellActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
  >
) => Promise<void>

export type SyncURLQueryFromQueryParamsObjectDispatcher = (
  location: Location,
  updatedURLQueryParams: Types.TempVars.Data.URLQueryParams,
  deletedURLQueryParams?: Types.TempVars.Data.URLQueryParams
) => SyncURLQueryFromQueryParamsObjectActionCreator

export type SyncURLQueryFromTempVarsDispatcher = (
  location: Location,
  tempVars: Types.TempVars.Data.Template[],
  deletedTempVars: Types.TempVars.Data.Template[],
  urlQueryParamsTimeRanges?: Types.TempVars.Data.URLQueryParams
) => SyncURLQueryFromQueryParamsObjectActionCreator

export type SyncURLQueryFromQueryParamsObjectActionCreator = (
  dispatch: Dispatch<LocationAction>
) => void

export type SyncDashboardTempVarsFromURLQueryParamsDispatcher = (
  dispatch: Dispatch<
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | TemplateVariableSelectedAction
  >,
  getState: () => Types.Dashboards.Reducers.Dashboards &
    Types.Dashboards.Reducers.Auth
) => void

export type SyncDashboardTimeRangeFromURLQueryParamsDispatcher = (
  dispatch: Dispatch<
    Types.Notifications.Actions.PublishNotificationActionCreator
  >,
  getState: () => Types.Dashboards.Reducers.Dashboards &
    Types.Dashboards.Reducers.DashTimeV1
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
  dispatch: Dispatch<
    Types.Notifications.Actions.PublishNotificationActionCreator
  >
) => Promise<void>
