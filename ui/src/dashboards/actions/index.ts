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

import {Dispatch} from 'redux'
import {InjectedRouter} from 'react-router'
import {Location} from 'history'
import {AxiosResponse} from 'axios'
// import * as AllData from 'src/types'
import {
  Cell,
  Dashboard,
  Source,
  Template,
  TemplateType,
  TimeRange,
  URLQueryParams,
} from 'src/types'
import * as DashboardData from 'src/types/dashboard'
import * as DashboardActions from 'src/types/actions/dashboards'
import * as DashboardAPIs from 'src/types/apis/dashboard'
import * as DashboardReducers from 'src/types/reducers/dashboards'
import * as AuthReducers from 'src/types/reducers/auth'
import * as NotificationActions from 'src/shared/actions/notifications'
import * as ErrorActions from 'src/types/actions/error'
import {LocationAction} from 'react-router-redux'

export const loadDashboards: DashboardActions.LoadDashboardsActionCreator = (
  dashboards: Dashboard[],
  dashboardID?: number
): DashboardActions.LoadDashboardsAction => ({
  type: 'LOAD_DASHBOARDS',
  payload: {
    dashboards,
    dashboardID,
  },
})

export const loadDashboard: DashboardActions.LoadDashboardActionCreator = (
  dashboard: Dashboard
): DashboardActions.LoadDashboardAction => ({
  type: 'LOAD_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const setDashTimeV1: DashboardActions.SetDashTimeV1ActionCreator = (
  dashboardID: number,
  timeRange: TimeRange
): DashboardActions.SetDashTimeV1Action => ({
  type: 'SET_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
    timeRange,
  },
})

export const retainRangesDashTimeV1: DashboardActions.RetainRangesDashTimeV1ActionCreator = (
  dashboardIDs: string[]
): DashboardActions.RetainRangesDashTimeV1Action => ({
  type: 'RETAIN_RANGES_DASHBOARD_TIME_V1',
  payload: {dashboardIDs},
})

export const setTimeRange: DashboardActions.SetTimeRangeActionCreator = (
  timeRange: TimeRange
): DashboardActions.SetTimeRangeAction => ({
  type: 'SET_DASHBOARD_TIME_RANGE',
  payload: {
    timeRange,
  },
})

export const setZoomedTimeRange: DashboardActions.SetZoomedTimeRangeActionCreator = (
  zoomedTimeRange: TimeRange
): DashboardActions.SetZoomedTimeRangeAction => ({
  type: 'SET_DASHBOARD_ZOOMED_TIME_RANGE',
  payload: {
    zoomedTimeRange,
  },
})

export const updateDashboard: DashboardActions.UpdateDashboardActionCreator = (
  dashboard: Dashboard
): DashboardActions.UpdateDashboardAction => ({
  type: 'UPDATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const createDashboard: DashboardActions.CreateDashboardActionCreator = (
  dashboard: Dashboard
): DashboardActions.CreateDashboardAction => ({
  type: 'CREATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const deleteDashboard: DashboardActions.DeleteDashboardActionCreator = (
  dashboard: Dashboard
): DashboardActions.DeleteDashboardAction => ({
  type: 'DELETE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const deleteDashboardFailed: DashboardActions.DeleteDashboardFailedActionCreator = (
  dashboard: Dashboard
): DashboardActions.DeleteDashboardFailedAction => ({
  type: 'DELETE_DASHBOARD_FAILED',
  payload: {
    dashboard,
  },
})

export const syncDashboardCell: DashboardActions.SyncDashboardCellActionCreator = (
  dashboard: Dashboard,
  cell: Cell
): DashboardActions.SyncDashboardCellAction => ({
  type: 'SYNC_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const addDashboardCell: DashboardActions.AddDashboardCellActionCreator = (
  dashboard: Dashboard,
  cell: Cell
): DashboardActions.AddDashboardCellAction => ({
  type: 'ADD_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const deleteDashboardCell: DashboardActions.DeleteDashboardCellActionCreator = (
  dashboard: Dashboard,
  cell: Cell
): DashboardActions.DeleteDashboardCellAction => ({
  type: 'DELETE_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const editCellQueryStatus: DashboardActions.EditCellQueryStatusActionCreator = (
  queryID: string,
  status: string
): DashboardActions.EditCellQueryStatusAction => ({
  type: 'EDIT_CELL_QUERY_STATUS',
  payload: {
    queryID,
    status,
  },
})

export const templateVariableSelected: DashboardActions.TemplateVariableSelectedActionCreator = (
  dashboardID: number,
  templateID: string,
  values
): DashboardActions.TemplateVariableSelectedAction => ({
  type: 'TEMPLATE_VARIABLE_SELECTED',
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

export const templateVariablesSelectedByName: DashboardActions.TemplateVariablesSelectedByNameActionCreator = (
  dashboardID: number,
  queryParams: URLQueryParams
): DashboardActions.TemplateVariablesSelectedByNameAction => ({
  type: 'TEMPLATE_VARIABLES_SELECTED_BY_NAME',
  payload: {
    dashboardID,
    queryParams,
  },
})

export const editTemplateVariableValues: DashboardActions.EditTemplateVariableValuesActionCreator = (
  dashboardID: number,
  templateID: string,
  values
): DashboardActions.EditTemplateVariableValuesAction => ({
  type: 'EDIT_TEMPLATE_VARIABLE_VALUES',
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

export const setHoverTime: DashboardActions.SetHoverTimeActionCreator = (
  hoverTime: string
): DashboardActions.SetHoverTimeAction => ({
  type: 'SET_HOVER_TIME',
  payload: {
    hoverTime,
  },
})

export const setActiveCell: DashboardActions.SetActiveCellActionCreator = (
  activeCellID: string
): DashboardActions.SetActiveCellAction => ({
  type: 'SET_ACTIVE_CELL',
  payload: {
    activeCellID,
  },
})

// Async Action Creators

export const getDashboardsAsync: DashboardActions.GetDashboardsDispatcher = (): DashboardActions.GetDashboardsThunk => async (
  dispatch: Dispatch<
    | DashboardActions.LoadDashboardsActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
): Promise<Dashboard[] | void> => {
  try {
    const {
      data: {dashboards},
    } = (await getDashboardsAJAX()) as AxiosResponse<
      DashboardAPIs.DashboardsResponse
    >
    dispatch(loadDashboards(dashboards))
    return dashboards
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

// gets update-to-date names of dashboards, but does not dispatch action
// in order to avoid duplicate and out-of-sync state problems in redux
export const getDashboardsNamesAsync: DashboardActions.GetDashboardsNamesDispatcher = (
  sourceID: string
): DashboardActions.GetDashboardsNamesThunk => async (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>
): Promise<DashboardData.DashboardName[] | void> => {
  try {
    // TODO: change this from getDashboardsAJAX to getDashboardsNamesAJAX
    // to just get dashboard names (and links) as api view call when that
    // view API is implemented (issue #3594), rather than getting whole
    // dashboard for each
    const {
      data: {dashboards},
    } = (await getDashboardsAJAX()) as AxiosResponse<
      DashboardAPIs.DashboardsResponse
    >
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

export const putDashboard = (
  dashboard: Dashboard
): DashboardActions.PutDashboardThunk => async (
  dispatch: Dispatch<
    | DashboardActions.UpdateDashboardAction
    | ErrorActions.ErrorThrownActionCreator
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

export const putDashboardByID: DashboardActions.PutDashboardByIDDispatcher = (
  dashboardID: number
): DashboardActions.PutDashboardByIDThunk => async (
  dispatch: Dispatch<ErrorActions.ErrorThrownActionCreator>,
  getState: () => DashboardReducers.Dashboards
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

export const updateDashboardCell: DashboardActions.UpdateDashboardCellDispatcher = (
  dashboard: Dashboard,
  cell: Cell
): DashboardActions.UpdateDashboardCellThunk => async (
  dispatch: Dispatch<
    | DashboardActions.SyncDashboardCellActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
): Promise<void> => {
  try {
    const {data} = await updateDashboardCellAJAX(cell)
    dispatch(syncDashboardCell(dashboard, data))
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const deleteDashboardAsync: DashboardActions.DeleteDashboardDispatcher = (
  dashboard: Dashboard
): DashboardActions.DeleteDashboardThunk => async (
  dispatch: Dispatch<
    | DashboardActions.DeleteDashboardActionCreator
    | NotificationActions.PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
    | DashboardActions.DeleteDashboardFailedActionCreator
  >
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

export const addDashboardCellAsync: DashboardActions.AddDashboardCellDispatcher = (
  dashboard: Dashboard,
  cellType?: DashboardData.CellType
): DashboardActions.AddDashboardCellThunk => async (
  dispatch: Dispatch<
    | DashboardActions.AddDashboardCellAction
    | NotificationActions.PublishNotificationActionCreator
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

export const cloneDashboardCellAsync: DashboardActions.CloneDashboardCellDispatcher = (
  dashboard: Dashboard,
  cell: Cell
): DashboardActions.CloneDashboardCellThunk => async (
  dispatch: Dispatch<
    | DashboardActions.AddDashboardCellActionCreator
    | NotificationActions.PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
): Promise<void> => {
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

export const deleteDashboardCellAsync: DashboardActions.DeleteDashboardCellDispatcher = (
  dashboard: Dashboard,
  cell: Cell
): DashboardActions.DeleteDashboardCellThunk => async (
  dispatch: Dispatch<
    | DashboardActions.DeleteDashboardCellActionCreator
    | NotificationActions.PublishNotificationActionCreator
    | ErrorActions.ErrorThrownActionCreator
  >
): Promise<void> => {
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
    } = (await getDashboardsAJAX()) as AxiosResponse<
      DashboardAPIs.DashboardsResponse
    >
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

export const syncURLQueryParamsFromQueryParamsObject = (
  location: Location,
  updatedURLQueryParams: URLQueryParams,
  deletedURLQueryParams: URLQueryParams = {}
): DashboardActions.SyncURLQueryFromQueryParamsObjectActionCreator => (
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

export const syncURLQueryFromTempVars: DashboardActions.SyncURLQueryFromTempVarsDispatcher = (
  location: Location,
  tempVars: Template[],
  deletedTempVars: Template[] = [],
  urlQueryParamsTimeRanges?: URLQueryParams
): DashboardActions.SyncURLQueryFromQueryParamsObjectActionCreator => (
  dispatch: Dispatch<
    DashboardActions.SyncURLQueryFromQueryParamsObjectDispatcher
  >
): void => {
  const updatedURLQueryParams = generateURLQueryParamsFromTempVars(tempVars)
  const deletedURLQueryParams = generateURLQueryParamsFromTempVars(
    deletedTempVars
  )

  let updatedURLQueryParamsWithTimeRange = {
    ...updatedURLQueryParams,
  }

  if (urlQueryParamsTimeRanges) {
    updatedURLQueryParamsWithTimeRange = {
      ...updatedURLQueryParamsWithTimeRange,
      ...urlQueryParamsTimeRanges,
    }
  }

  syncURLQueryParamsFromQueryParamsObject(
    location,
    updatedURLQueryParamsWithTimeRange,
    deletedURLQueryParams
  )(dispatch)
}

const syncDashboardTempVarsFromURLQueryParams = (
  dashboardID: number,
  urlQueryParams: URLQueryParams
): DashboardActions.SyncDashboardTempVarsFromURLQueryParamsDispatcher => (
  dispatch: Dispatch<
    | NotificationActions.PublishNotificationActionCreator
    | DashboardActions.TemplateVariableSelectedAction
  >,
  getState: () => DashboardReducers.Dashboards & AuthReducers.Auth
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

const syncDashboardTimeRangeFromURLQueryParams = (
  dashboardID: number,
  urlQueryParams: URLQueryParams,
  location: Location
): DashboardActions.SyncDashboardTimeRangeFromURLQueryParamsDispatcher => (
  dispatch: Dispatch<NotificationActions.PublishNotificationActionCreator>,
  getState: () => DashboardReducers.Dashboards & DashboardReducers.DashTimeV1
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

const syncDashboardFromURLQueryParams = (
  dashboardID: number,
  location: Location
): DashboardActions.SyncDashboardFromURLQueryParamsDispatcher => (
  dispatch: Dispatch<
    | DashboardActions.SyncDashboardTempVarsFromURLQueryParamsDispatcher
    | DashboardActions.SyncDashboardTimeRangeFromURLQueryParamsDispatcher
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

export const getDashboardWithHydratedAndSyncedTempVarsAsync: DashboardActions.GetDashboardWithHydratedAndSyncedTempVarsAsyncDispatcher = (
  dashboardID: number,
  source: Source,
  router: InjectedRouter,
  location: Location
): DashboardActions.GetDashboardWithHydratedAndSyncedTempVarsAsyncThunk => async (
  dispatch: Dispatch<NotificationActions.PublishNotificationActionCreator>
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

export const setZoomedTimeRangeAsync: DashboardActions.SetZoomedTimeRangeDispatcher = (
  zoomedTimeRange: TimeRange,
  location: Location
): DashboardActions.SetZoomedTimeRangeThunk => async (
  dispatch: Dispatch<
    | DashboardActions.SetZoomedTimeRangeActionCreator
    | DashboardActions.SyncURLQueryFromQueryParamsObjectDispatcher
  >
): Promise<void> => {
  dispatch(setZoomedTimeRange(zoomedTimeRange))
  const urlQueryParamsZoomedTimeRange = {
    zoomedLower: zoomedTimeRange.lower,
    zoomedUpper: zoomedTimeRange.upper,
  }

  syncURLQueryParamsFromQueryParamsObject(
    location,
    urlQueryParamsZoomedTimeRange
  )(dispatch)
}
