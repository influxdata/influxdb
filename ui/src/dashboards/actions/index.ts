import {bindActionCreators} from 'redux'
import {replace} from 'react-router-redux'
import _ from 'lodash'
import queryString from 'query-string'

import {proxy} from 'src/utils/queryUrlGenerator'
import {isUserAuthorized, EDITOR_ROLE} from 'src/auth/Authorized'
import {parseMetaQuery} from 'src/tempVars/utils/parsing'

import {
  getDashboards as getDashboardsAJAX,
  getDashboard as getDashboardAJAX,
  updateDashboard as updateDashboardAJAX,
  deleteDashboard as deleteDashboardAJAX,
  updateDashboardCell as updateDashboardCellAJAX,
  addDashboardCell as addDashboardCellAJAX,
  deleteDashboardCell as deleteDashboardCellAJAX,
  createDashboard as createDashboardAJAX,
} from 'src/dashboards/apis'
import {getMe} from 'src/shared/apis/auth'

import {notify} from 'src/shared/actions/notifications'
import {errorThrown} from 'src/shared/actions/errors'

import {
  generateURLQueryParamsFromTempVars,
  findUpdatedTempVarsInURLQueryParams,
  findInvalidTempVarsInURLQuery,
} from 'src/dashboards/utils/tempVars'
import {validTimeRange, validAbsoluteTimeRange} from 'src/dashboards/utils/time'
import {
  getNewDashboardCell,
  getClonedDashboardCell,
} from 'src/dashboards/utils/cellGetters'
import {
  notifyDashboardDeleted,
  notifyDashboardDeleteFailed,
  notifyCellAdded,
  notifyCellDeleted,
  notifyDashboardImportFailed,
  notifyDashboardImported,
  notifyDashboardNotFound,
  notifyInvalidTempVarValueInURLQuery,
  notifyInvalidZoomedTimeRangeValueInURLQuery,
  notifyInvalidTimeRangeValueInURLQuery,
  notifyViewerUnauthorizedToSetTempVars,
} from 'src/shared/copy/notifications'

import {makeQueryForTemplate} from 'src/dashboards/utils/tempVars'
import {getDeep} from 'src/utils/wrappers'

import idNormalizer, {TYPE_ID} from 'src/normalizers/id'

import {defaultTimeRange} from 'src/shared/data/timeRanges'

import {InjectedRouter} from 'react-router'
import {Location} from 'history'
import {Dispatch} from 'redux'
import {
  Cell,
  Dashboard,
  Me,
  Source,
  Template,
  TemplateType,
  TimeRange,
  URLQueryParams,
} from 'src/types'
import {CellType, DashboardName} from 'src/types/dashboard'
import {TimeRangeOption} from 'src/shared/data/timeRanges'
import {PublishNotificationActionCreator} from 'src/shared/actions/notifications'
import * as ErrorActions from 'src/shared/actions/errors'
import {LocationAction} from 'react-router-redux'

export type LoadDashboardsActionCreator = (
  dashboards: Dashboard[],
  dashboardID?: number
) => LoadDashboardsAction

interface LoadDashboardsAction {
  type: 'LOAD_DASHBOARDS'
  payload: {
    dashboards: Dashboard[]
    dashboardID: number
  }
}
export const loadDashboards: LoadDashboardsActionCreator = (
  dashboards: Dashboard[],
  dashboardID?: number
): LoadDashboardsAction => ({
  type: 'LOAD_DASHBOARDS',
  payload: {
    dashboards,
    dashboardID,
  },
})

interface LoadDashboardAction {
  type: 'LOAD_DASHBOARD'
  payload: {
    dashboard: Dashboard
  }
}
export const loadDashboard = (dashboard: Dashboard): LoadDashboardAction => ({
  type: 'LOAD_DASHBOARD',
  payload: {
    dashboard,
  },
})

interface SetDashTimeV1Action {
  type: 'SET_DASHBOARD_TIME_V1'
  payload: {
    dashboardID: number
    timeRange: TimeRange
  }
}
export const setDashTimeV1 = (
  dashboardID: number,
  timeRange: TimeRange
): SetDashTimeV1Action => ({
  type: 'SET_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
    timeRange,
  },
})

interface RetainRangesDashTimeV1Action {
  type: 'RETAIN_RANGES_DASHBOARD_TIME_V1'
  payload: {
    dashboardIDs: string[]
  }
}
export const retainRangesDashTimeV1 = (
  dashboardIDs: string[]
): RetainRangesDashTimeV1Action => ({
  type: 'RETAIN_RANGES_DASHBOARD_TIME_V1',
  payload: {dashboardIDs},
})

export type SetTimeRangeActionCreator = (
  timeRange: TimeRange
) => SetTimeRangeAction

interface SetTimeRangeAction {
  type: 'SET_DASHBOARD_TIME_RANGE'
  payload: {
    timeRange: TimeRange
  }
}
export const setTimeRange = (timeRange: TimeRange): SetTimeRangeAction => ({
  type: 'SET_DASHBOARD_TIME_RANGE',
  payload: {
    timeRange,
  },
})

interface SetZoomedTimeRangeAction {
  type: 'SET_DASHBOARD_ZOOMED_TIME_RANGE'
  payload: {
    zoomedTimeRange: TimeRange
  }
}
export const setZoomedTimeRange = (
  zoomedTimeRange: TimeRange
): SetZoomedTimeRangeAction => ({
  type: 'SET_DASHBOARD_ZOOMED_TIME_RANGE',
  payload: {
    zoomedTimeRange,
  },
})

interface UpdateDashboardAction {
  type: 'UPDATE_DASHBOARD'
  payload: {
    dashboard: Dashboard
  }
}
export const updateDashboard = (
  dashboard: Dashboard
): UpdateDashboardAction => ({
  type: 'UPDATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

interface CreateDashboardAction {
  type: 'CREATE_DASHBOARD'
  payload: {
    dashboard: Dashboard
  }
}
export const createDashboard = (
  dashboard: Dashboard
): CreateDashboardAction => ({
  type: 'CREATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

interface DeleteDashboardAction {
  type: 'DELETE_DASHBOARD'
  payload: {
    dashboard: Dashboard
    dashboardID: number
  }
}
export const deleteDashboard = (
  dashboard: Dashboard
): DeleteDashboardAction => ({
  type: 'DELETE_DASHBOARD',
  payload: {
    dashboard,
    dashboardID: dashboard.id,
  },
})

interface DeleteDashboardFailedAction {
  type: 'DELETE_DASHBOARD_FAILED'
  payload: {
    dashboard: Dashboard
  }
}
export const deleteDashboardFailed = (
  dashboard: Dashboard
): DeleteDashboardFailedAction => ({
  type: 'DELETE_DASHBOARD_FAILED',
  payload: {
    dashboard,
  },
})

interface SyncDashboardCellAction {
  type: 'SYNC_DASHBOARD_CELL'
  payload: {
    dashboard: Dashboard
    cell: Cell
  }
}
export const syncDashboardCell = (
  dashboard: Dashboard,
  cell: Cell
): SyncDashboardCellAction => ({
  type: 'SYNC_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

interface AddDashboardCellAction {
  type: 'ADD_DASHBOARD_CELL'
  payload: {
    dashboard: Dashboard
    cell: Cell
  }
}
export const addDashboardCell = (
  dashboard: Dashboard,
  cell: Cell
): AddDashboardCellAction => ({
  type: 'ADD_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

interface DeleteDashboardCellAction {
  type: 'DELETE_DASHBOARD_CELL'
  payload: {
    dashboard: Dashboard
    cell: Cell
  }
}
export const deleteDashboardCell = (
  dashboard: Dashboard,
  cell: Cell
): DeleteDashboardCellAction => ({
  type: 'DELETE_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

interface EditCellQueryStatusAction {
  type: 'EDIT_CELL_QUERY_STATUS'
  payload: {
    queryID: string
    status: string
  }
}
export const editCellQueryStatus = (
  queryID: string,
  status: string
): EditCellQueryStatusAction => ({
  type: 'EDIT_CELL_QUERY_STATUS',
  payload: {
    queryID,
    status,
  },
})

interface TemplateVariableSelectedAction {
  type: 'TEMPLATE_VARIABLE_SELECTED'
  payload: {
    dashboardID: number
    templateID: string
    values: any[]
  }
}
export const templateVariableSelected = (
  dashboardID: number,
  templateID: string,
  values
): TemplateVariableSelectedAction => ({
  type: 'TEMPLATE_VARIABLE_SELECTED',
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

interface TemplateVariablesSelectedByNameAction {
  type: 'TEMPLATE_VARIABLES_SELECTED_BY_NAME'
  payload: {
    dashboardID: number
    queryParams: URLQueryParams
  }
}
export const templateVariablesSelectedByName = (
  dashboardID: number,
  queryParams: URLQueryParams
): TemplateVariablesSelectedByNameAction => ({
  type: 'TEMPLATE_VARIABLES_SELECTED_BY_NAME',
  payload: {
    dashboardID,
    queryParams,
  },
})

interface EditTemplateVariableValuesAction {
  type: 'EDIT_TEMPLATE_VARIABLE_VALUES'
  payload: {
    dashboardID: number
    templateID: string
    values: any[]
  }
}
export const editTemplateVariableValues = (
  dashboardID: number,
  templateID: string,
  values
): EditTemplateVariableValuesAction => ({
  type: 'EDIT_TEMPLATE_VARIABLE_VALUES',
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

interface SetHoverTimeAction {
  type: 'SET_HOVER_TIME'
  payload: {
    hoverTime: string
  }
}
export const setHoverTime = (hoverTime: string): SetHoverTimeAction => ({
  type: 'SET_HOVER_TIME',
  payload: {
    hoverTime,
  },
})

interface SetActiveCellAction {
  type: 'SET_ACTIVE_CELL'
  payload: {
    activeCellID: string
  }
}
export const setActiveCell = (activeCellID: string): SetActiveCellAction => ({
  type: 'SET_ACTIVE_CELL',
  payload: {
    activeCellID,
  },
})

// Async Action Creators

export type GetDashboardsDispatcher = () => GetDashboardsThunk

type GetDashboardsThunk = (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>
) => Promise<Dashboard[] | void>

export const getDashboardsAsync = (): GetDashboardsThunk => async (
  dispatch: Dispatch<
    LoadDashboardsActionCreator | ErrorActions.ErrorThrownActionCreator
  >
): Promise<Dashboard[] | void> => {
  try {
    const {
      data: {dashboards},
    } = await getDashboardsAJAX()
    dispatch(loadDashboards(dashboards))
    return dashboards
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export type GetDashboardsNamesDispatcher = (
  sourceID: string
) => GetDashboardsNamesThunk

type GetDashboardsNamesThunk = (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>
) => Promise<DashboardName[] | void>

// gets update-to-date names of dashboards, but does not dispatch action
// in order to avoid duplicate and out-of-sync state problems in redux
export const getDashboardsNamesAsync = (
  sourceID: string
): GetDashboardsNamesThunk => async (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>
): Promise<DashboardName[] | void> => {
  try {
    // TODO: change this from getDashboardsAJAX to getDashboardsNamesAJAX
    // to just get dashboard names (and links) as api view call when that
    // view API is implemented (issue #3594), rather than getting whole
    // dashboard for each
    const {
      data: {dashboards},
    } = await getDashboardsAJAX()
    const dashboardsNames = dashboards.map(({id, name}) => ({
      id,
      name,
      link: `/sources/${sourceID}/dashboards/${id}`,
    }))
    return dashboardsNames
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const getDashboardAsync = (dashboardID: number) => async (
  dispatch
): Promise<Dashboard | null> => {
  try {
    const {data: dashboard} = await getDashboardAJAX(dashboardID)
    dispatch(loadDashboard(dashboard))
    return dashboard
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
    return null
  }
}

export const getChronografVersion = () => async (): Promise<string | void> => {
  try {
    const results = await getMe()
    const version = _.get(results, 'headers.x-chronograf-version')
    return version
  } catch (error) {
    console.error(error)
  }
}

const removeUnselectedTemplateValues = (dashboard: Dashboard): Template[] => {
  const templates = getDeep<Template[]>(dashboard, 'templates', []).map(
    template => {
      if (template.type === TemplateType.CSV) {
        return template
      }

      const value = template.values.find(val => val.selected)
      const values = value ? [value] : []

      return {...template, values}
    }
  )
  return templates
}

export type PutDashboardDispatcher = (dashboard: Dashboard) => PutDashboardThunk

type PutDashboardThunk = (
  dispatch: Dispatch<
    UpdateDashboardAction | ErrorActions.ErrorThrownActionCreator
  >
) => Promise<void>

export const putDashboard = (dashboard: Dashboard): PutDashboardThunk => async (
  dispatch: Dispatch<
    UpdateDashboardAction | ErrorActions.ErrorThrownActionCreator
  >
): Promise<void> => {
  try {
    // save only selected template values to server
    const templatesWithOnlySelectedValues = removeUnselectedTemplateValues(
      dashboard
    )
    const {
      data: dashboardWithOnlySelectedTemplateValues,
    } = await updateDashboardAJAX({
      ...dashboard,
      templates: templatesWithOnlySelectedValues,
    })
    // save all template values to redux
    dispatch(
      updateDashboard({
        ...dashboardWithOnlySelectedTemplateValues,
        templates: dashboard.templates,
      })
    )
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

interface DashboardsReducerState {
  dashboardUI: {dashboards: Dashboard[]}
}

type PutDashboardByIDThunk = (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>,
  getState: () => DashboardsReducerState
) => Promise<void>

export type PutDashboardByIDDispatcher = (
  dashboardID: number
) => PutDashboardByIDThunk

export const putDashboardByID = (
  dashboardID: number
): PutDashboardByIDThunk => async (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>,
  getState: () => DashboardsReducerState
): Promise<void> => {
  try {
    const {
      dashboardUI: {dashboards},
    } = getState()
    const dashboard: Dashboard = dashboards.find(d => d.id === +dashboardID)
    const templates = removeUnselectedTemplateValues(dashboard)
    await updateDashboardAJAX({...dashboard, templates})
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const updateDashboardCell = (dashboard: Dashboard, cell: Cell) => async (
  dispatch
): Promise<void> => {
  try {
    const {data} = await updateDashboardCellAJAX(cell)
    dispatch(syncDashboardCell(dashboard, data))
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const deleteDashboardAsync = (dashboard: Dashboard) => async (
  dispatch
): Promise<void> => {
  dispatch(deleteDashboard(dashboard))
  try {
    await deleteDashboardAJAX(dashboard)
    dispatch(notify(notifyDashboardDeleted(dashboard.name)))
  } catch (error) {
    dispatch(
      errorThrown(
        error,
        notifyDashboardDeleteFailed(dashboard.name, error.data.message)
      )
    )
    dispatch(deleteDashboardFailed(dashboard))
  }
}

export type AddDashboardCellDispatcher = (
  dashboard: Dashboard,
  cellType: CellType
) => AddDashboardCellThunk

type AddDashboardCellThunk = (
  dispatch: Dispatch<
    | AddDashboardCellAction
    | PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
) => Promise<void>

export const addDashboardCellAsync = (
  dashboard: Dashboard,
  cellType: CellType
): AddDashboardCellThunk => async (
  dispatch: Dispatch<
    | AddDashboardCellAction
    | PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
): Promise<void> => {
  try {
    const {data} = await addDashboardCellAJAX(
      dashboard,
      getNewDashboardCell(dashboard, cellType)
    )
    dispatch(addDashboardCell(dashboard, data))
    dispatch(notify(notifyCellAdded(data.name)))
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const cloneDashboardCellAsync = (
  dashboard: Dashboard,
  cell: Cell
) => async (dispatch): Promise<void> => {
  try {
    const clonedCell = getClonedDashboardCell(dashboard, cell)
    const {data} = await addDashboardCellAJAX(dashboard, clonedCell)
    dispatch(addDashboardCell(dashboard, data))
    dispatch(notify(notifyCellAdded(clonedCell.name)))
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const deleteDashboardCellAsync = (
  dashboard: Dashboard,
  cell: Cell
) => async (dispatch): Promise<void> => {
  try {
    await deleteDashboardCellAJAX(cell)
    dispatch(deleteDashboardCell(dashboard, cell))
    dispatch(notify(notifyCellDeleted(cell.name)))
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const importDashboardAsync = (dashboard: Dashboard) => async (
  dispatch
): Promise<void> => {
  try {
    // save only selected template values to server
    const templatesWithOnlySelectedValues = removeUnselectedTemplateValues(
      dashboard
    )

    const results = await createDashboardAJAX({
      ...dashboard,
      templates: templatesWithOnlySelectedValues,
    })

    const dashboardWithOnlySelectedTemplateValues = _.get(results, 'data')

    // save all template values to redux
    dispatch(
      createDashboard({
        ...dashboardWithOnlySelectedTemplateValues,
        templates: dashboard.templates,
      })
    )

    const {
      data: {dashboards},
    } = await getDashboardsAJAX()
    dispatch(loadDashboards(dashboards))

    dispatch(notify(notifyDashboardImported(name)))
  } catch (error) {
    const errorMessage = _.get(
      error,
      'data.message',
      'Could not upload dashboard'
    )
    dispatch(notify(notifyDashboardImportFailed('', errorMessage)))
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const hydrateTempVarValuesAsync = (
  dashboardID: number,
  source: Source
) => async (dispatch, getState): Promise<void> => {
  try {
    const dashboard = getState().dashboardUI.dashboards.find(
      d => d.id === dashboardID
    )
    const templates: Template[] = dashboard.templates
    const queries = templates
      .filter(
        template => getDeep<string>(template, 'query.influxql', '') !== ''
      )
      .map(async template => {
        const query = makeQueryForTemplate(template.query)
        const response = await proxy({source: source.links.proxy, query})
        const values = parseMetaQuery(query, response.data)

        return {template, values}
      })
    const results = await Promise.all(queries)

    for (const {template, values} of results) {
      dispatch(editTemplateVariableValues(+dashboard.id, template.id, values))
    }
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

const removeNullValues = obj => _.pickBy(obj, o => o)

type SyncURLQueryFromQueryParamsObjectDispatcher = (
  location: Location,
  updatedURLQueryParams: URLQueryParams,
  deletedURLQueryParams: URLQueryParams
) => SyncURLQueryFromQueryParamsObjectActionCreator

type SyncURLQueryFromQueryParamsObjectActionCreator = (
  dispatch: Dispatch<LocationAction>
) => void

export const syncURLQueryParamsFromQueryParamsObject = (
  location: Location,
  updatedURLQueryParams: URLQueryParams,
  deletedURLQueryParams: URLQueryParams = {}
): SyncURLQueryFromQueryParamsObjectActionCreator => (
  dispatch: Dispatch<LocationAction>
): void => {
  const updatedLocationQuery = removeNullValues({
    ...location.query,
    ...updatedURLQueryParams,
  })

  _.each(deletedURLQueryParams, (__, k) => {
    delete updatedLocationQuery[k]
  })

  const updatedSearchString = queryString.stringify(updatedLocationQuery)
  const updatedSearch = {search: updatedSearchString}
  const updatedLocation = {
    ...location,
    query: updatedLocationQuery,
    ...updatedSearch,
  }

  dispatch(replace(updatedLocation))
}

export type SyncURLQueryFromTempVarsDispatcher = (
  location: Location,
  tempVars: Template[],
  deletedTempVars: Template[],
  urlQueryParamsTimeRanges: URLQueryParams
) => SyncURLQueryFromQueryParamsObjectActionCreator

export const syncURLQueryFromTempVars = (
  location: Location,
  tempVars: Template[],
  deletedTempVars: Template[] = [],
  urlQueryParamsTimeRanges: URLQueryParams = {}
): SyncURLQueryFromQueryParamsObjectActionCreator => (
  dispatch: Dispatch<SyncURLQueryFromQueryParamsObjectDispatcher>
): void => {
  const updatedURLQueryParams = generateURLQueryParamsFromTempVars(tempVars)
  const deletedURLQueryParams = generateURLQueryParamsFromTempVars(
    deletedTempVars
  )

  const updatedURLQueryParamsWithTimeRange = {
    ...updatedURLQueryParams,
    ...urlQueryParamsTimeRanges,
  }

  syncURLQueryParamsFromQueryParamsObject(
    location,
    updatedURLQueryParamsWithTimeRange,
    deletedURLQueryParams
  )(dispatch)
}

interface AuthReducerState {
  auth: {isUsingAuth: boolean; me: Me}
}
type SyncDashboardTempVarsFromURLQueryParamsDispatcher = (
  dispatch: Dispatch<
    PublishNotificationActionCreator | TemplateVariableSelectedAction
  >,
  getState: () => DashboardsReducerState & AuthReducerState
) => void
const syncDashboardTempVarsFromURLQueryParams = (
  dashboardID: number,
  urlQueryParams: URLQueryParams
): SyncDashboardTempVarsFromURLQueryParamsDispatcher => (
  dispatch: Dispatch<
    PublishNotificationActionCreator | TemplateVariableSelectedAction
  >,
  getState: () => DashboardsReducerState & AuthReducerState
): void => {
  const {
    dashboardUI,
    auth: {isUsingAuth, me},
  } = getState()
  const dashboard = dashboardUI.dashboards.find(d => d.id === dashboardID)

  // viewers are not currently allowed to select temp vars and/or use overrides
  if (isUsingAuth && !isUserAuthorized(me.role, EDITOR_ROLE)) {
    const urlQueryParamsTempVarsWithUpdatedValues = findUpdatedTempVarsInURLQueryParams(
      dashboard.templates,
      urlQueryParams
    )
    if (urlQueryParamsTempVarsWithUpdatedValues.length) {
      dispatch(notify(notifyViewerUnauthorizedToSetTempVars()))
      return
    }
  }

  const urlQueryParamsTempVarsWithInvalidValues = findInvalidTempVarsInURLQuery(
    dashboard.templates,
    urlQueryParams
  )
  urlQueryParamsTempVarsWithInvalidValues.forEach(invalidURLQuery => {
    dispatch(notify(notifyInvalidTempVarValueInURLQuery(invalidURLQuery)))
  })

  dispatch(templateVariablesSelectedByName(dashboardID, urlQueryParams))
}

type DashTimeV1Range = TimeRangeOption & {dashboardID: number}

interface DashTimeV1ReducerState {
  dashTimeV1: {ranges: DashTimeV1Range[]}
}

type SyncDashboardTimeRangeFromURLQueryParamsDispatcher = (
  dispatch: Dispatch<PublishNotificationActionCreator>,
  getState: () => DashboardsReducerState & DashTimeV1ReducerState
) => void

const syncDashboardTimeRangeFromURLQueryParams = (
  dashboardID: number,
  urlQueryParams: URLQueryParams,
  location: Location
): SyncDashboardTimeRangeFromURLQueryParamsDispatcher => (
  dispatch: Dispatch<PublishNotificationActionCreator>,
  getState: () => DashboardsReducerState & DashTimeV1ReducerState
): void => {
  const {
    dashboardUI: {dashboards},
    dashTimeV1,
  } = getState()
  const dashboard = dashboards.find(d => d.id === dashboardID)

  const timeRangeFromQueries = {
    lower: urlQueryParams.lower,
    upper: urlQueryParams.upper,
  }
  const zoomedTimeRangeFromQueries = {
    lower: urlQueryParams.zoomedLower,
    upper: urlQueryParams.zoomedUpper,
  }

  let validatedTimeRange = validTimeRange(timeRangeFromQueries)
  if (!validatedTimeRange.lower) {
    const dashboardTimeRange = dashTimeV1.ranges.find(
      r => r.dashboardID === idNormalizer(TYPE_ID, dashboardID)
    )

    validatedTimeRange = dashboardTimeRange || defaultTimeRange

    if (timeRangeFromQueries.lower || timeRangeFromQueries.upper) {
      dispatch(notify(notifyInvalidTimeRangeValueInURLQuery()))
    }
  }
  dispatch(setDashTimeV1(dashboardID, validatedTimeRange))

  const validatedZoomedTimeRange = validAbsoluteTimeRange(
    zoomedTimeRangeFromQueries
  )
  if (
    !validatedZoomedTimeRange.lower &&
    (urlQueryParams.zoomedLower || urlQueryParams.zoomedUpper)
  ) {
    dispatch(notify(notifyInvalidZoomedTimeRangeValueInURLQuery()))
  }
  dispatch(setZoomedTimeRange(validatedZoomedTimeRange))
  const urlQueryParamsTimeRanges = {
    lower: validatedTimeRange.lower,
    upper: validatedTimeRange.upper,
    zoomedLower: validatedZoomedTimeRange.lower,
    zoomedUpper: validatedZoomedTimeRange.upper,
  }

  syncURLQueryFromTempVars(
    location,
    dashboard.templates,
    [],
    urlQueryParamsTimeRanges
  )(dispatch)
}

type SyncDashboardFromURLQueryParamsDispatcher = (
  dispatch: Dispatch<
    | SyncDashboardTempVarsFromURLQueryParamsDispatcher
    | SyncDashboardTimeRangeFromURLQueryParamsDispatcher
  >
) => void
const syncDashboardFromURLQueryParams = (
  dashboardID: number,
  location: Location
): SyncDashboardFromURLQueryParamsDispatcher => (
  dispatch: Dispatch<
    | SyncDashboardTempVarsFromURLQueryParamsDispatcher
    | SyncDashboardTimeRangeFromURLQueryParamsDispatcher
  >
): void => {
  const urlQueryParams = queryString.parse(window.location.search)
  bindActionCreators(syncDashboardTempVarsFromURLQueryParams, dispatch)(
    dashboardID,
    urlQueryParams
  )

  bindActionCreators(syncDashboardTimeRangeFromURLQueryParams, dispatch)(
    dashboardID,
    urlQueryParams,
    location
  )
}

export type GetDashboardWithHydratedAndSyncedTempVarsAsyncDispatcher = (
  dashboardID: string,
  source: Source,
  router: InjectedRouter,
  location: Location
) => GetDashboardWithHydratedAndSyncedTempVarsAsyncActionCreator

type GetDashboardWithHydratedAndSyncedTempVarsAsyncActionCreator = (
  dispatch: Dispatch<PublishNotificationActionCreator>
) => Promise<void>

export const getDashboardWithHydratedAndSyncedTempVarsAsync = (
  dashboardID: number,
  source: Source,
  router: InjectedRouter,
  location: Location
): GetDashboardWithHydratedAndSyncedTempVarsAsyncActionCreator => async (
  dispatch: Dispatch<PublishNotificationActionCreator>
): Promise<void> => {
  const dashboard = await bindActionCreators(getDashboardAsync, dispatch)(
    dashboardID
  )
  if (!dashboard) {
    router.push(`/sources/${source.id}/dashboards`)
    dispatch(notify(notifyDashboardNotFound(dashboardID)))
    return
  }

  await bindActionCreators(hydrateTempVarValuesAsync, dispatch)(
    +dashboardID,
    source
  )

  bindActionCreators(syncDashboardFromURLQueryParams, dispatch)(
    +dashboardID,
    location
  )
}

export const setZoomedTimeRangeAsync = (
  zoomedTimeRange: TimeRange,
  location: Location
) => async (dispatch): Promise<void> => {
  dispatch(setZoomedTimeRange(zoomedTimeRange))
  const urlQueryParamsZoomedTimeRange = {
    zoomedLower: zoomedTimeRange.lower,
    zoomedUpper: zoomedTimeRange.upper,
  }
  dispatch(
    syncURLQueryParamsFromQueryParamsObject(
      location,
      urlQueryParamsZoomedTimeRange
    )
  )
}
