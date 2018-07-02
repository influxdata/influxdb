import {replace, RouterAction} from 'react-router-redux'
import _ from 'lodash'
import qs from 'qs'

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
import {hydrateTemplate, isTemplateNested} from 'src/tempVars/apis'

import {notify} from 'src/shared/actions/notifications'
import {errorThrown} from 'src/shared/actions/errors'

import {
  applySelections,
  templateSelectionsFromQueryParams,
  queryParamsFromTemplates,
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
  notifyInvalidZoomedTimeRangeValueInURLQuery,
  notifyInvalidTimeRangeValueInURLQuery,
} from 'src/shared/copy/notifications'

import {getDeep} from 'src/utils/wrappers'

import idNormalizer, {TYPE_ID} from 'src/normalizers/id'

import {defaultTimeRange} from 'src/shared/data/timeRanges'

// Types
import {Dispatch} from 'redux'
import {AxiosResponse} from 'axios'
import * as DashboardsActions from 'src/types/actions/dashboards'
import * as DashboardsApis from 'src/types/apis/dashboards'
import * as DashboardsModels from 'src/types/dashboards'
import * as DashboardsReducers from 'src/types/reducers/dashboards'
import * as ErrorsActions from 'src/types/actions/errors'
import * as QueriesModels from 'src/types/queries'
import * as SourcesModels from 'src/types/sources'
import * as TempVarsModels from 'src/types/tempVars'
import * as NotificationsActions from 'src/types/actions/notifications'

export const loadDashboards: DashboardsActions.LoadDashboardsActionCreator = (
  dashboards: DashboardsModels.Dashboard[],
  dashboardID?: number
): DashboardsActions.LoadDashboardsAction => ({
  type: 'LOAD_DASHBOARDS',
  payload: {
    dashboards,
    dashboardID,
  },
})

export const loadDashboard: DashboardsActions.LoadDashboardActionCreator = (
  dashboard: DashboardsModels.Dashboard
): DashboardsActions.LoadDashboardAction => ({
  type: 'LOAD_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const setDashTimeV1: DashboardsActions.SetDashTimeV1ActionCreator = (
  dashboardID: number,
  timeRange: QueriesModels.TimeRange
): DashboardsActions.SetDashTimeV1Action => ({
  type: 'SET_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
    timeRange,
  },
})

export const retainRangesDashTimeV1: DashboardsActions.RetainRangesDashTimeV1ActionCreator = (
  dashboardIDs: string[]
): DashboardsActions.RetainRangesDashTimeV1Action => ({
  type: 'RETAIN_RANGES_DASHBOARD_TIME_V1',
  payload: {dashboardIDs},
})

export const setTimeRange: DashboardsActions.SetTimeRangeActionCreator = (
  timeRange: QueriesModels.TimeRange
): DashboardsActions.SetTimeRangeAction => ({
  type: 'SET_DASHBOARD_TIME_RANGE',
  payload: {
    timeRange,
  },
})

export const setZoomedTimeRange: DashboardsActions.SetZoomedTimeRangeActionCreator = (
  zoomedTimeRange: QueriesModels.TimeRange
): DashboardsActions.SetZoomedTimeRangeAction => ({
  type: 'SET_DASHBOARD_ZOOMED_TIME_RANGE',
  payload: {
    zoomedTimeRange,
  },
})

export const updateDashboard: DashboardsActions.UpdateDashboardActionCreator = (
  dashboard: DashboardsModels.Dashboard
): DashboardsActions.UpdateDashboardAction => ({
  type: 'UPDATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const createDashboard: DashboardsActions.CreateDashboardActionCreator = (
  dashboard: DashboardsModels.Dashboard
): DashboardsActions.CreateDashboardAction => ({
  type: 'CREATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const deleteDashboard: DashboardsActions.DeleteDashboardActionCreator = (
  dashboard: DashboardsModels.Dashboard
): DashboardsActions.DeleteDashboardAction => ({
  type: 'DELETE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const deleteDashboardFailed: DashboardsActions.DeleteDashboardFailedActionCreator = (
  dashboard: DashboardsModels.Dashboard
): DashboardsActions.DeleteDashboardFailedAction => ({
  type: 'DELETE_DASHBOARD_FAILED',
  payload: {
    dashboard,
  },
})

export const syncDashboardCell: DashboardsActions.SyncDashboardCellActionCreator = (
  dashboard: DashboardsModels.Dashboard,
  cell: DashboardsModels.Cell
): DashboardsActions.SyncDashboardCellAction => ({
  type: 'SYNC_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const addDashboardCell: DashboardsActions.AddDashboardCellActionCreator = (
  dashboard: DashboardsModels.Dashboard,
  cell: DashboardsModels.Cell
): DashboardsActions.AddDashboardCellAction => ({
  type: 'ADD_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const deleteDashboardCell: DashboardsActions.DeleteDashboardCellActionCreator = (
  dashboard: DashboardsModels.Dashboard,
  cell: DashboardsModels.Cell
): DashboardsActions.DeleteDashboardCellAction => ({
  type: 'DELETE_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const editCellQueryStatus: DashboardsActions.EditCellQueryStatusActionCreator = (
  queryID: string,
  status: string
): DashboardsActions.EditCellQueryStatusAction => ({
  type: 'EDIT_CELL_QUERY_STATUS',
  payload: {
    queryID,
    status,
  },
})

export const templateVariableLocalSelected: DashboardsActions.TemplateVariableLocalSelectedActionCreator = (
  dashboardID: number,
  templateID: string,
  values
): DashboardsActions.TemplateVariableLocalSelectedAction => ({
  type: 'TEMPLATE_VARIABLE_LOCAL_SELECTED',
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

export const updateTemplates = (
  templates: TempVarsModels.Template[]
): DashboardsActions.UpdateTemplatesAction => ({
  type: 'UPDATE_TEMPLATES',
  payload: {templates},
})

export const setHoverTime: DashboardsActions.SetHoverTimeActionCreator = (
  hoverTime: string
): DashboardsActions.SetHoverTimeAction => ({
  type: 'SET_HOVER_TIME',
  payload: {
    hoverTime,
  },
})

export const setActiveCell: DashboardsActions.SetActiveCellActionCreator = (
  activeCellID: string
): DashboardsActions.SetActiveCellAction => ({
  type: 'SET_ACTIVE_CELL',
  payload: {
    activeCellID,
  },
})

const getDashboard = (
  state,
  dashboardId: number
): DashboardsModels.Dashboard => {
  const dashboard = state.dashboardUI.dashboards.find(
    d => d.id === +dashboardId
  )

  if (!dashboard) {
    throw new Error(`Could not find dashboard with id '${dashboardId}'`)
  }

  return dashboard
}

// Async Action Creators

export const getDashboardsAsync: DashboardsActions.GetDashboardsDispatcher = (): DashboardsActions.GetDashboardsThunk => async (
  dispatch: Dispatch<
    | DashboardsActions.LoadDashboardsActionCreator
    | ErrorsActions.ErrorThrownActionCreator
  >
): Promise<DashboardsModels.Dashboard[] | void> => {
  try {
    const {
      data: {dashboards},
    } = (await getDashboardsAJAX()) as AxiosResponse<
      DashboardsApis.DashboardsResponse
    >
    dispatch(loadDashboards(dashboards))
    return dashboards
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
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
  dashboard: DashboardsModels.Dashboard
): TempVarsModels.Template[] => {
  const templates = getDeep<TempVarsModels.Template[]>(
    dashboard,
    'templates',
    []
  ).map(template => {
    if (
      template.type === TempVarsModels.TemplateType.CSV ||
      template.type === TempVarsModels.TemplateType.Map
    ) {
      return template
    }

    const value = template.values.find(val => val.selected)
    const values = value ? [value] : []

    return {...template, values}
  })
  return templates
}

export const putDashboard = (
  dashboard: DashboardsModels.Dashboard
): DashboardsActions.PutDashboardThunk => async (
  dispatch: Dispatch<
    | DashboardsActions.UpdateDashboardAction
    | ErrorsActions.ErrorThrownActionCreator
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

export const putDashboardByID: DashboardsActions.PutDashboardByIDDispatcher = (
  dashboardID: number
): DashboardsActions.PutDashboardByIDThunk => async (
  dispatch: Dispatch<ErrorsActions.ErrorThrownActionCreator>,
  getState: () => DashboardsReducers.Dashboards
): Promise<void> => {
  try {
    const dashboard = getDashboard(getState(), dashboardID)
    const templates = removeUnselectedTemplateValues(dashboard)
    await updateDashboardAJAX({...dashboard, templates})
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const updateDashboardCell: DashboardsActions.UpdateDashboardCellDispatcher = (
  dashboard: DashboardsModels.Dashboard,
  cell: DashboardsModels.Cell
): DashboardsActions.UpdateDashboardCellThunk => async (
  dispatch: Dispatch<
    | DashboardsActions.SyncDashboardCellActionCreator
    | ErrorsActions.ErrorThrownActionCreator
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

export const deleteDashboardAsync: DashboardsActions.DeleteDashboardDispatcher = (
  dashboard: DashboardsModels.Dashboard
): DashboardsActions.DeleteDashboardThunk => async (
  dispatch: Dispatch<
    | DashboardsActions.DeleteDashboardActionCreator
    | NotificationsActions.PublishNotificationActionCreator
    | ErrorsActions.ErrorThrownActionCreator
    | DashboardsActions.DeleteDashboardFailedActionCreator
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

export const addDashboardCellAsync: DashboardsActions.AddDashboardCellDispatcher = (
  dashboard: DashboardsModels.Dashboard,
  cellType?: DashboardsModels.CellType
): DashboardsActions.AddDashboardCellThunk => async (
  dispatch: Dispatch<
    | DashboardsActions.AddDashboardCellAction
    | NotificationsActions.PublishNotificationActionCreator
    | ErrorsActions.ErrorThrownActionCreator
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

export const cloneDashboardCellAsync: DashboardsActions.CloneDashboardCellDispatcher = (
  dashboard: DashboardsModels.Dashboard,
  cell: DashboardsModels.Cell
): DashboardsActions.CloneDashboardCellThunk => async (
  dispatch: Dispatch<
    | DashboardsActions.AddDashboardCellActionCreator
    | NotificationsActions.PublishNotificationActionCreator
    | ErrorsActions.ErrorThrownActionCreator
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

export const deleteDashboardCellAsync: DashboardsActions.DeleteDashboardCellDispatcher = (
  dashboard: DashboardsModels.Dashboard,
  cell: DashboardsModels.Cell
): DashboardsActions.DeleteDashboardCellThunk => async (
  dispatch: Dispatch<
    | DashboardsActions.DeleteDashboardCellActionCreator
    | NotificationsActions.PublishNotificationActionCreator
    | ErrorsActions.ErrorThrownActionCreator
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
  dashboard: DashboardsModels.Dashboard
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
      DashboardsApis.DashboardsResponse
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

const updateTimeRangeFromQueryParams = (dashboardID: number) => (
  dispatch,
  getState
): void => {
  const {dashTimeV1} = getState()
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
    (queryParams.zoomedLower || queryParams.zoomedUpper)
  ) {
    dispatch(notify(notifyInvalidZoomedTimeRangeValueInURLQuery()))
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

export const getDashboardWithTemplatesAsync = (
  dashboardId: number,
  source: SourcesModels.Source
) => async (dispatch: Dispatch<any>): Promise<void> => {
  let dashboard: DashboardsModels.Dashboard

  try {
    const resp = await getDashboardAJAX(dashboardId)
    dashboard = resp.data
  } catch {
    dispatch(replace(`/sources/${source.id}/dashboards`))
    dispatch(notify(notifyDashboardNotFound(dashboardId)))

    return
  }

  const templateSelections = templateSelectionsFromQueryParams()
  const proxyLink = source.links.proxy
  const nonNestedTemplates = await Promise.all(
    dashboard.templates
      .filter(t => !isTemplateNested(t))
      .map(t => hydrateTemplate(proxyLink, t, []))
  )

  applySelections(nonNestedTemplates, templateSelections)

  const nestedTemplates = await Promise.all(
    dashboard.templates
      .filter(t => isTemplateNested(t))
      .map(t => hydrateTemplate(proxyLink, t, nonNestedTemplates))
  )

  applySelections(nestedTemplates, templateSelections)

  const templates = [...nonNestedTemplates, ...nestedTemplates]

  // TODO: Notify if any of the supplied query params were invalid
  dispatch(loadDashboard({...dashboard, templates}))
  dispatch<any>(updateTemplateQueryParams(dashboardId))
  dispatch<any>(updateTimeRangeFromQueryParams(dashboardId))
}

export const rehydrateNestedTemplatesAsync = (
  dashboardId: number,
  source: SourcesModels.Source
) => async (dispatch: Dispatch<any>, getState): Promise<void> => {
  const dashboard = getDashboard(getState(), dashboardId)
  const proxyLink = source.links.proxy
  const templateSelections = templateSelectionsFromQueryParams()
  const nestedTemplates = await Promise.all(
    dashboard.templates
      .filter(t => isTemplateNested(t))
      .map(t => hydrateTemplate(proxyLink, t, dashboard.templates))
  )

  applySelections(nestedTemplates, templateSelections)

  dispatch(updateTemplates(nestedTemplates))
  dispatch<any>(updateTemplateQueryParams(dashboardId))
}

export const updateTemplateQueryParams = (dashboardId: number) => (
  dispatch,
  getState
): void => {
  const templates = getDashboard(getState(), dashboardId).templates
  const updatedQueryParams = {
    tempVars: queryParamsFromTemplates(templates),
  }

  dispatch(updateQueryParams(updatedQueryParams))
}

export const updateQueryParams = (updatedQueryParams: object): RouterAction => {
  const {search, pathname} = window.location

  const newQueryParams = _.pickBy(
    {
      ...qs.parse(search, {ignoreQueryPrefix: true}),
      ...updatedQueryParams,
    },
    v => !!v
  )

  const newSearch = qs.stringify(newQueryParams)
  const newLocation = {pathname, search: `?${newSearch}`}

  return replace(newLocation)
}
