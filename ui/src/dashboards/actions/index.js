import _ from 'lodash'

import {
  getDashboards as getDashboardsAJAX,
  updateDashboard as updateDashboardAJAX,
  deleteDashboard as deleteDashboardAJAX,
  updateDashboardCell as updateDashboardCellAJAX,
  addDashboardCell as addDashboardCellAJAX,
  deleteDashboardCell as deleteDashboardCellAJAX,
  runTemplateVariableQuery,
} from 'src/dashboards/apis'
import {getMe} from 'src/shared/apis/auth'

import {notify} from 'shared/actions/notifications'
import {errorThrown} from 'shared/actions/errors'

import {
  getNewDashboardCell,
  getClonedDashboardCell,
} from 'src/dashboards/utils/cellGetters'
import {
  notifyDashboardDeleted,
  notifyDashboardDeleteFailed,
  notifyCellAdded,
  notifyCellDeleted,
} from 'shared/copy/notifications'

import {
  TEMPLATE_VARIABLE_SELECTED,
  TEMPLATE_VARIABLES_SELECTED_BY_NAME,
} from 'shared/constants/actionTypes'
import {makeQueryForTemplate} from 'src/dashboards/utils/templateVariableQueryGenerator'
import parsers from 'shared/parsing'

export const loadDashboards = (dashboards, dashboardID) => ({
  type: 'LOAD_DASHBOARDS',
  payload: {
    dashboards,
    dashboardID,
  },
})

export const loadDeafaultDashTimeV1 = dashboardID => ({
  type: 'ADD_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
  },
})

export const addDashTimeV1 = (dashboardID, timeRange) => ({
  type: 'ADD_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
    timeRange,
  },
})

export const setDashTimeV1 = (dashboardID, timeRange) => ({
  type: 'SET_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
    timeRange,
  },
})

export const setTimeRange = timeRange => ({
  type: 'SET_DASHBOARD_TIME_RANGE',
  payload: {
    timeRange,
  },
})

export const updateDashboard = dashboard => ({
  type: 'UPDATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const deleteDashboard = dashboard => ({
  type: 'DELETE_DASHBOARD',
  payload: {
    dashboard,
    dashboardID: dashboard.id,
  },
})

export const deleteDashboardFailed = dashboard => ({
  type: 'DELETE_DASHBOARD_FAILED',
  payload: {
    dashboard,
  },
})

export const updateDashboardCells = (dashboard, cells) => ({
  type: 'UPDATE_DASHBOARD_CELLS',
  payload: {
    dashboard,
    cells,
  },
})

export const syncDashboardCell = (dashboard, cell) => ({
  type: 'SYNC_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const addDashboardCell = (dashboard, cell) => ({
  type: 'ADD_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const editDashboardCell = (dashboard, x, y, isEditing) => ({
  type: 'EDIT_DASHBOARD_CELL',
  // x and y coords are used as a alternative to cell ids, which are not
  // universally unique, and cannot be because React depends on a
  // quasi-predictable ID for keys. Since cells cannot overlap, coordinates act
  // as a suitable id
  payload: {
    dashboard,
    x, // x-coord of the cell to be edited
    y, // y-coord of the cell to be edited
    isEditing,
  },
})

export const cancelEditCell = (dashboardID, cellID) => ({
  type: 'CANCEL_EDIT_CELL',
  payload: {
    dashboardID,
    cellID,
  },
})

export const renameDashboardCell = (dashboard, x, y, name) => ({
  type: 'RENAME_DASHBOARD_CELL',
  payload: {
    dashboard,
    x, // x-coord of the cell to be renamed
    y, // y-coord of the cell to be renamed
    name,
  },
})

export const deleteDashboardCell = (dashboard, cell) => ({
  type: 'DELETE_DASHBOARD_CELL',
  payload: {
    dashboard,
    cell,
  },
})

export const editCellQueryStatus = (queryID, status) => ({
  type: 'EDIT_CELL_QUERY_STATUS',
  payload: {
    queryID,
    status,
  },
})

export const templateVariableSelected = (dashboardID, templateID, values) => ({
  type: TEMPLATE_VARIABLE_SELECTED,
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

export const templateVariablesSelectedByName = (dashboardID, query) => ({
  type: TEMPLATE_VARIABLES_SELECTED_BY_NAME,
  payload: {
    dashboardID,
    query,
  },
})

export const editTemplateVariableValues = (
  dashboardID,
  templateID,
  values
) => ({
  type: 'EDIT_TEMPLATE_VARIABLE_VALUES',
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

export const setHoverTime = hoverTime => ({
  type: 'SET_HOVER_TIME',
  payload: {
    hoverTime,
  },
})

export const setActiveCell = activeCellID => ({
  type: 'SET_ACTIVE_CELL',
  payload: {
    activeCellID,
  },
})

// Async Action Creators

export const getDashboardsAsync = () => async dispatch => {
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

export const getChronografVersion = () => async () => {
  try {
    const results = await getMe()
    const version = _.get(results, 'headers.x-chronograf-version')
    return version
  } catch (error) {
    console.error(error)
  }
}

const removeUnselectedTemplateValues = dashboard => {
  const templates = dashboard.templates.map(template => {
    if (template.type === 'csv') {
      return template
    }

    const value = template.values.find(val => val.selected)
    const values = value ? [value] : []

    return {...template, values}
  })
  return templates
}

export const putDashboard = dashboard => async dispatch => {
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

export const putDashboardByID = dashboardID => async (dispatch, getState) => {
  try {
    const {
      dashboardUI: {dashboards},
    } = getState()
    const dashboard = dashboards.find(d => d.id === +dashboardID)
    const templates = removeUnselectedTemplateValues(dashboard)
    await updateDashboardAJAX({...dashboard, templates})
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const updateDashboardCell = (dashboard, cell) => async dispatch => {
  try {
    const {data} = await updateDashboardCellAJAX(cell)
    dispatch(syncDashboardCell(dashboard, data))
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const deleteDashboardAsync = dashboard => async dispatch => {
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
  dashboard,
  cellType
) => async dispatch => {
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

export const cloneDashboardCellAsync = (dashboard, cell) => async dispatch => {
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

export const deleteDashboardCellAsync = (dashboard, cell) => async dispatch => {
  try {
    await deleteDashboardCellAJAX(cell)
    dispatch(deleteDashboardCell(dashboard, cell))
    dispatch(notify(notifyCellDeleted(cell.name)))
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

export const updateTempVarValues = (source, dashboard) => async dispatch => {
  try {
    const tempsWithQueries = dashboard.templates.filter(
      ({query}) => !!query.influxql
    )

    const asyncQueries = tempsWithQueries.map(({query}) =>
      runTemplateVariableQuery(source, {query: makeQueryForTemplate(query)})
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
