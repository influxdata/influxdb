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

// Types
import {Dispatch} from 'redux'
import {InjectedRouter} from 'react-router'
import {Location} from 'history'
import {AxiosResponse} from 'axios'
import {LocationAction} from 'react-router-redux'
import * as Types from 'src/types/modules'

export const loadDashboards: Types.Dashboards.Actions.LoadDashboardsActionCreator = (
  dashboards: Types.Dashboards.Data.Dashboard[],
  dashboardID?: number
): Types.Dashboards.Actions.LoadDashboardsAction => ({
  type: 'LOAD_DASHBOARDS',
  payload: {
    dashboards,
    dashboardID,
  },
})

export const loadDashboard: Types.Dashboards.Actions.LoadDashboardActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
): Types.Dashboards.Actions.LoadDashboardAction => ({
  type: 'LOAD_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const setDashTimeV1: Types.Dashboards.Actions.SetDashTimeV1ActionCreator = (
  dashboardID: number,
  timeRange: Types.Queries.Data.TimeRange
): Types.Dashboards.Actions.SetDashTimeV1Action => ({
  type: 'SET_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
    timeRange,
  },
})

export const retainRangesDashTimeV1: Types.Dashboards.Actions.RetainRangesDashTimeV1ActionCreator = (
  dashboardIDs: string[]
): Types.Dashboards.Actions.RetainRangesDashTimeV1Action => ({
  type: 'RETAIN_RANGES_DASHBOARD_TIME_V1',
  payload: {dashboardIDs},
})

export const setTimeRange: Types.Dashboards.Actions.SetTimeRangeActionCreator = (
  timeRange: Types.Queries.Data.TimeRange
): Types.Dashboards.Actions.SetTimeRangeAction => ({
  type: 'SET_DASHBOARD_TIME_RANGE',
  payload: {
    timeRange,
  },
})

export const setZoomedTimeRange: Types.Dashboards.Actions.SetZoomedTimeRangeActionCreator = (
  zoomedTimeRange: Types.Queries.Data.TimeRange
): Types.Dashboards.Actions.SetZoomedTimeRangeAction => ({
  type: 'SET_DASHBOARD_ZOOMED_TIME_RANGE',
  payload: {
    zoomedTimeRange,
  },
})

export const updateDashboard: Types.Dashboards.Actions.UpdateDashboardActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
): Types.Dashboards.Actions.UpdateDashboardAction => ({
  type: 'UPDATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const createDashboard: Types.Dashboards.Actions.CreateDashboardActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
): Types.Dashboards.Actions.CreateDashboardAction => ({
  type: 'CREATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const deleteDashboard: Types.Dashboards.Actions.DeleteDashboardActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
): Types.Dashboards.Actions.DeleteDashboardAction => ({
  type: 'DELETE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const deleteDashboardFailed: Types.Dashboards.Actions.DeleteDashboardFailedActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard
): Types.Dashboards.Actions.DeleteDashboardFailedAction => ({
  type: 'DELETE_DASHBOARD_FAILED',
  payload: {
    dashboard,
  },
})

export const syncDashboardCell: Types.Dashboards.Actions.SyncDashboardCellActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
): Types.Dashboards.Actions.SyncDashboardCellAction => ({
  type: 'SYNC_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const addDashboardCell: Types.Dashboards.Actions.AddDashboardCellActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
): Types.Dashboards.Actions.AddDashboardCellAction => ({
  type: 'ADD_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const deleteDashboardCell: Types.Dashboards.Actions.DeleteDashboardCellActionCreator = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
): Types.Dashboards.Actions.DeleteDashboardCellAction => ({
  type: 'DELETE_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const editCellQueryStatus: Types.Dashboards.Actions.EditCellQueryStatusActionCreator = (
  queryID: string,
  status: string
): Types.Dashboards.Actions.EditCellQueryStatusAction => ({
  type: 'EDIT_CELL_QUERY_STATUS',
  payload: {
    queryID,
    status,
  },
})

export const templateVariableSelected: Types.Dashboards.Actions.TemplateVariableSelectedActionCreator = (
  dashboardID: number,
  templateID: string,
  values
): Types.Dashboards.Actions.TemplateVariableSelectedAction => ({
  type: 'TEMPLATE_VARIABLE_SELECTED',
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

export const templateVariablesSelectedByName: Types.Dashboards.Actions.TemplateVariablesSelectedByNameActionCreator = (
  dashboardID: number,
  queryParams: Types.TempVars.Data.URLQueryParams
): Types.Dashboards.Actions.TemplateVariablesSelectedByNameAction => ({
  type: 'TEMPLATE_VARIABLES_SELECTED_BY_NAME',
  payload: {
    dashboardID,
    queryParams,
  },
})

export const editTemplateVariableValues: Types.Dashboards.Actions.EditTemplateVariableValuesActionCreator = (
  dashboardID: number,
  templateID: string,
  values
): Types.Dashboards.Actions.EditTemplateVariableValuesAction => ({
  type: 'EDIT_TEMPLATE_VARIABLE_VALUES',
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

export const setHoverTime: Types.Dashboards.Actions.SetHoverTimeActionCreator = (
  hoverTime: string
): Types.Dashboards.Actions.SetHoverTimeAction => ({
  type: 'SET_HOVER_TIME',
  payload: {
    hoverTime,
  },
})

export const setActiveCell: Types.Dashboards.Actions.SetActiveCellActionCreator = (
  activeCellID: string
): Types.Dashboards.Actions.SetActiveCellAction => ({
  type: 'SET_ACTIVE_CELL',
  payload: {
    activeCellID,
  },
})

// Async Action Creators

export const getDashboardsAsync: Types.Dashboards.Actions.GetDashboardsDispatcher = (): Types.Dashboards.Actions.GetDashboardsThunk => async (
  dispatch: Dispatch<
    | Types.Dashboards.Actions.LoadDashboardsActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
  >
): Promise<Types.Dashboards.Data.Dashboard[] | void> => {
  try {
    const {
      data: {dashboards},
    } = (await getDashboardsAJAX()) as AxiosResponse<
      Types.Dashboards.Apis.DashboardsResponse
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
export const getDashboardsNamesAsync: Types.Dashboards.Actions.GetDashboardsNamesDispatcher = (
  sourceID: string
): Types.Dashboards.Actions.GetDashboardsNamesThunk => async (
  dispatch: Dispatch<Types.Errors.Actions.ErrorThrownActionCreator>
): Promise<Types.Dashboards.Data.DashboardName[] | void> => {
  try {
    // TODO: change this from getDashboardsAJAX to getDashboardsNamesAJAX
    // to just get dashboard names (and links) as api view call when that
    // view API is implemented (issue #3594), rather than getting whole
    // dashboard for each
    const {
      data: {dashboards},
    } = (await getDashboardsAJAX()) as AxiosResponse<
      Types.Dashboards.Apis.DashboardsResponse
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
): Promise<Types.Dashboards.Data.Dashboard | null> => {
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

const removeUnselectedTemplateValues = (
  dashboard: Types.Dashboards.Data.Dashboard
): Types.TempVars.Data.Template[] => {
  const templates = getDeep<Types.TempVars.Data.Template[]>(
    dashboard,
    'templates',
    []
  ).map(template => {
    if (template.type === Types.TempVars.Data.TemplateType.CSV) {
      return template
    }

    const value = template.values.find(val => val.selected)
    const values = value ? [value] : []

    return {...template, values}
  })
  return templates
}

export const putDashboard = (
  dashboard: Types.Dashboards.Data.Dashboard
): Types.Dashboards.Actions.PutDashboardThunk => async (
  dispatch: Dispatch<
    | Types.Dashboards.Actions.UpdateDashboardAction
    | Types.Errors.Actions.ErrorThrownActionCreator
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

export const putDashboardByID: Types.Dashboards.Actions.PutDashboardByIDDispatcher = (
  dashboardID: number
): Types.Dashboards.Actions.PutDashboardByIDThunk => async (
  dispatch: Dispatch<Types.Errors.Actions.ErrorThrownActionCreator>,
  getState: () => Types.Dashboards.Reducers.Dashboards
): Promise<void> => {
  try {
    const {
      dashboardUI: {dashboards},
    } = getState()
    const dashboard: Types.Dashboards.Data.Dashboard = dashboards.find(
      d => d.id === +dashboardID
    )
    const templates = removeUnselectedTemplateValues(dashboard)
    await updateDashboardAJAX({...dashboard, templates})
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const updateDashboardCell: Types.Dashboards.Actions.UpdateDashboardCellDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
): Types.Dashboards.Actions.UpdateDashboardCellThunk => async (
  dispatch: Dispatch<
    | Types.Dashboards.Actions.SyncDashboardCellActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
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

export const deleteDashboardAsync: Types.Dashboards.Actions.DeleteDashboardDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard
): Types.Dashboards.Actions.DeleteDashboardThunk => async (
  dispatch: Dispatch<
    | Types.Dashboards.Actions.DeleteDashboardActionCreator
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
    | Types.Dashboards.Actions.DeleteDashboardFailedActionCreator
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

export const addDashboardCellAsync: Types.Dashboards.Actions.AddDashboardCellDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cellType?: Types.Dashboards.Data.CellType
): Types.Dashboards.Actions.AddDashboardCellThunk => async (
  dispatch: Dispatch<
    | Types.Dashboards.Actions.AddDashboardCellAction
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
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

export const cloneDashboardCellAsync: Types.Dashboards.Actions.CloneDashboardCellDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
): Types.Dashboards.Actions.CloneDashboardCellThunk => async (
  dispatch: Dispatch<
    | Types.Dashboards.Actions.AddDashboardCellActionCreator
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
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

export const deleteDashboardCellAsync: Types.Dashboards.Actions.DeleteDashboardCellDispatcher = (
  dashboard: Types.Dashboards.Data.Dashboard,
  cell: Types.Dashboards.Data.Cell
): Types.Dashboards.Actions.DeleteDashboardCellThunk => async (
  dispatch: Dispatch<
    | Types.Dashboards.Actions.DeleteDashboardCellActionCreator
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | Types.Errors.Actions.ErrorThrownActionCreator
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

export const importDashboardAsync = (
  dashboard: Types.Dashboards.Data.Dashboard
) => async (dispatch): Promise<void> => {
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
      Types.Dashboards.Apis.DashboardsResponse
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
  source: Types.Sources.Data.Source
) => async (dispatch, getState): Promise<void> => {
  try {
    const dashboard = getState().dashboardUI.dashboards.find(
      d => d.id === dashboardID
    )
    const templates: Types.TempVars.Data.Template[] = dashboard.templates
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
  updatedURLQueryParams: Types.TempVars.Data.URLQueryParams,
  deletedURLQueryParams: Types.TempVars.Data.URLQueryParams = {}
): Types.Dashboards.Actions.SyncURLQueryFromQueryParamsObjectActionCreator => (
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

export const syncURLQueryFromTempVars: Types.Dashboards.Actions.SyncURLQueryFromTempVarsDispatcher = (
  location: Location,
  tempVars: Types.TempVars.Data.Template[],
  deletedTempVars: Types.TempVars.Data.Template[] = [],
  urlQueryParamsTimeRanges?: Types.TempVars.Data.URLQueryParams
): Types.Dashboards.Actions.SyncURLQueryFromQueryParamsObjectActionCreator => (
  dispatch: Dispatch<
    Types.Dashboards.Actions.SyncURLQueryFromQueryParamsObjectDispatcher
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
  urlQueryParams: Types.TempVars.Data.URLQueryParams
): Types.Dashboards.Actions.SyncDashboardTempVarsFromURLQueryParamsDispatcher => (
  dispatch: Dispatch<
    | Types.Notifications.Actions.PublishNotificationActionCreator
    | Types.Dashboards.Actions.TemplateVariableSelectedAction
  >,
  getState: () => Types.Dashboards.Reducers.Dashboards &
    Types.Auth.Reducers.Auth
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
  urlQueryParams: Types.TempVars.Data.URLQueryParams,
  location: Location
): Types.Dashboards.Actions.SyncDashboardTimeRangeFromURLQueryParamsDispatcher => (
  dispatch: Dispatch<
    Types.Notifications.Actions.PublishNotificationActionCreator
  >,
  getState: () => Types.Dashboards.Reducers.Dashboards &
    Types.Dashboards.Reducers.DashTimeV1
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
): Types.Dashboards.Actions.SyncDashboardFromURLQueryParamsDispatcher => (
  dispatch: Dispatch<
    | Types.Dashboards.Actions.SyncDashboardTempVarsFromURLQueryParamsDispatcher
    | Types.Dashboards.Actions.SyncDashboardTimeRangeFromURLQueryParamsDispatcher
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

export const getDashboardWithHydratedAndSyncedTempVarsAsync: Types.Dashboards.Actions.GetDashboardWithHydratedAndSyncedTempVarsAsyncDispatcher = (
  dashboardID: number,
  source: Types.Sources.Data.Source,
  router: InjectedRouter,
  location: Location
): Types.Dashboards.Actions.GetDashboardWithHydratedAndSyncedTempVarsAsyncThunk => async (
  dispatch: Dispatch<
    Types.Notifications.Actions.PublishNotificationActionCreator
  >
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

export const setZoomedTimeRangeAsync: Types.Dashboards.Actions.SetZoomedTimeRangeDispatcher = (
  zoomedTimeRange: Types.Queries.Data.TimeRange,
  location: Location
): Types.Dashboards.Actions.SetZoomedTimeRangeThunk => async (
  dispatch: Dispatch<
    | Types.Dashboards.Actions.SetZoomedTimeRangeActionCreator
    | Types.Dashboards.Actions.SyncURLQueryFromQueryParamsObjectDispatcher
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
