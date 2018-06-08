import _ from 'lodash'

import {
  getDashboards as getDashboardsAJAX,
  updateDashboard as updateDashboardAJAX,
  deleteDashboard as deleteDashboardAJAX,
  updateDashboardCell as updateDashboardCellAJAX,
  addDashboardCell as addDashboardCellAJAX,
  deleteDashboardCell as deleteDashboardCellAJAX,
  runTemplateVariableQuery,
  createDashboard as createDashboardAJAX,
} from 'src/dashboards/apis'
import {getMe} from 'src/shared/apis/auth'

import {notify} from 'src/shared/actions/notifications'
import {errorThrown} from 'src/shared/actions/errors'

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
} from 'src/shared/copy/notifications'

import {
  TEMPLATE_VARIABLE_SELECTED,
  TEMPLATE_VARIABLES_SELECTED_BY_NAME,
} from 'src/shared/constants/actionTypes'
import {CellType} from 'src/types/dashboard'
import {makeQueryForTemplate} from 'src/dashboards/utils/templateVariableQueryGenerator'
import parsers from 'src/shared/parsing'
import {getDeep} from 'src/utils/wrappers'

import {Dashboard, TimeRange, Cell, Source, Template} from 'src/types'

interface LoadDashboardsAction {
  type: 'LOAD_DASHBOARDS'
  payload: {
    dashboards: Dashboard[]
    dashboardID: number
  }
}

export const loadDashboards = (
  dashboards: Dashboard[],
  dashboardID?: number
): LoadDashboardsAction => ({
  type: 'LOAD_DASHBOARDS',
  payload: {
    dashboards,
    dashboardID,
  },
})

interface LoadDeafaultDashTimeV1Action {
  type: 'ADD_DASHBOARD_TIME_V1'
  payload: {
    dashboardID: number
  }
}

export const loadDeafaultDashTimeV1 = (
  dashboardID: number
): LoadDeafaultDashTimeV1Action => ({
  type: 'ADD_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
  },
})

interface AddDashTimeV1Action {
  type: 'ADD_DASHBOARD_TIME_V1'
  payload: {
    dashboardID: number
    timeRange: TimeRange
  }
}

export const addDashTimeV1 = (
  dashboardID: number,
  timeRange: TimeRange
): AddDashTimeV1Action => ({
  type: 'ADD_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
    timeRange,
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
  type: TEMPLATE_VARIABLE_SELECTED,
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

// This is limited in typing as it will be changed soon
export const templateVariablesSelectedByName = (
  dashboardID: number,
  query: any
) => ({
  type: TEMPLATE_VARIABLES_SELECTED_BY_NAME,
  payload: {
    dashboardID,
    query,
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

export const getDashboardsAsync = () => async (
  dispatch
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
      if (template.type === 'csv') {
        return template
      }

      const value = template.values.find(val => val.selected)
      const values = value ? [value] : []

      return {...template, values}
    }
  )
  return templates
}

export const putDashboard = (dashboard: Dashboard) => async (
  dispatch
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

export const putDashboardByID = (dashboardID: number) => async (
  dispatch,
  getState
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

export const addDashboardCellAsync = (
  dashboard: Dashboard,
  cellType: CellType
) => async (dispatch): Promise<void> => {
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

export const updateTempVarValues = (
  source: Source,
  dashboard: Dashboard
) => async (dispatch): Promise<void> => {
  try {
    const tempsWithQueries = dashboard.templates.filter(
      ({query}) => !!_.get(query, 'influxql')
    )

    const asyncQueries = tempsWithQueries.map(({query}) =>
      runTemplateVariableQuery(source, {
        query: makeQueryForTemplate(query),
        db: null,
        tempVars: null,
      })
    )

    const results = await Promise.all(asyncQueries)

    results.forEach(({data}, i) => {
      const {type, query, id} = tempsWithQueries[i]
      const parsed = parsers[type](data, query.tagKey || query.measurement)
      const vals = parsed[type]
      dispatch(editTemplateVariableValues(dashboard.id, id, vals))
    })
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
