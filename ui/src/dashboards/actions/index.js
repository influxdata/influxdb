import {bindActionCreators} from 'redux'
import {push} from 'react-router-redux'
import _ from 'lodash'
import queryString from 'query-string'

import {
  getDashboards as getDashboardsAJAX,
  getDashboard as getDashboardAJAX,
  updateDashboard as updateDashboardAJAX,
  deleteDashboard as deleteDashboardAJAX,
  updateDashboardCell as updateDashboardCellAJAX,
  addDashboardCell as addDashboardCellAJAX,
  deleteDashboardCell as deleteDashboardCellAJAX,
  getTempVarValuesBySourceQuery,
} from 'src/dashboards/apis'

import {notify} from 'shared/actions/notifications'
import {errorThrown} from 'shared/actions/errors'

import {
  generateURLQueryFromTempVars,
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
  notifyDashboardNotFound,
  notifyInvalidTempVarValueInURLQuery,
  notifyInvalidZoomedTimeRangeValueInURLQuery,
  notifyInvalidTimeRangeValueInURLQuery,
} from 'shared/copy/notifications'

import {makeQueryForTemplate} from 'src/dashboards/utils/tempVars'
import parsers from 'shared/parsing'

import idNormalizer, {TYPE_ID} from 'src/normalizers/id'

import {defaultTimeRange} from 'src/shared/data/timeRanges'

export const loadDashboards = (dashboards, dashboardID) => ({
  type: 'LOAD_DASHBOARDS',
  payload: {
    dashboards,
    dashboardID,
  },
})

export const loadDashboard = dashboard => ({
  type: 'LOAD_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const setDashTimeV1 = (dashboardID, timeRange) => ({
  type: 'SET_DASHBOARD_TIME_V1',
  payload: {
    dashboardID,
    timeRange,
  },
})

export const pruneDashTimeV1 = dashboardIDs => ({
  type: 'PRUNE_DASHBOARD_TIME_V1',
  payload: {dashboardIDs},
})

export const setTimeRange = timeRange => ({
  type: 'SET_DASHBOARD_TIME_RANGE',
  payload: {
    timeRange,
  },
})

export const setZoomedTimeRange = zoomedTimeRange => ({
  type: 'SET_DASHBOARD_ZOOMED_TIME_RANGE',
  payload: {
    zoomedTimeRange,
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
  type: 'TEMPLATE_VARIABLE_SELECTED',
  payload: {
    dashboardID,
    templateID,
    values,
  },
})

export const templateVariablesSelectedByName = (dashboardID, queries) => ({
  type: 'TEMPLATE_VARIABLES_SELECTED_BY_NAME',
  payload: {
    dashboardID,
    queries,
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

export const getDashboardAsync = dashboardID => async dispatch => {
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

export const hydrateTempVarValuesAsync = (dashboardID, source) => async (
  dispatch,
  getState
) => {
  try {
    const dashboard = getState().dashboardUI.dashboards.find(
      d => d.id === dashboardID
    )

    const tempsWithQueries = dashboard.templates.filter(
      ({query}) => !!query.influxql
    )

    const asyncQueries = tempsWithQueries.map(({query}) =>
      getTempVarValuesBySourceQuery(source, {
        query: makeQueryForTemplate(query),
      })
    )

    const results = await Promise.all(asyncQueries)

    results.forEach(({data}, i) => {
      const {type, query, id} = tempsWithQueries[i]
      const parsed = parsers[type](data, query.tagKey || query.measurement)
      const vals = parsed[type]
      dispatch(editTemplateVariableValues(+dashboard.id, id, vals))
    })
  } catch (error) {
    console.error(error)
    dispatch(errorThrown(error))
  }
}

const removeNullValues = obj => _.pickBy(obj, o => o)

export const syncURLQueryFromQueriesObject = (
  location,
  updatedURLQueries,
  deletedURLQueries = {}
) => dispatch => {
  const updatedLocationQuery = removeNullValues({
    ...location.query,
    ...updatedURLQueries,
  })

  _.each(deletedURLQueries, (v, k) => {
    delete updatedLocationQuery[k]
  })

  const updatedSearchString = queryString.stringify(updatedLocationQuery)
  const updatedSearch = {search: updatedSearchString}
  const updatedLocation = {
    ...location,
    query: updatedLocationQuery,
    ...updatedSearch,
  }

  dispatch(push(updatedLocation))
}

export const syncURLQueryFromTempVars = (
  location,
  tempVars,
  deletedTempVars = [],
  timeRange = {}
) => dispatch => {
  const updatedURLQueries = generateURLQueryFromTempVars(tempVars)
  const deletedURLQueries = generateURLQueryFromTempVars(deletedTempVars)

  const updatedURLQueriesWithTimeRange = {...updatedURLQueries, ...timeRange}

  dispatch(
    syncURLQueryFromQueriesObject(
      location,
      updatedURLQueriesWithTimeRange,
      deletedURLQueries
    )
  )
}

const syncDashboardTempVarsFromURLQueries = (dashboardID, urlQueries) => (
  dispatch,
  getState
) => {
  const dashboard = getState().dashboardUI.dashboards.find(
    d => d.id === dashboardID
  )

  const urlQueryTempVarsWithInvalidValues = findInvalidTempVarsInURLQuery(
    dashboard.templates,
    urlQueries
  )
  urlQueryTempVarsWithInvalidValues.forEach(invalidURLQuery => {
    dispatch(notify(notifyInvalidTempVarValueInURLQuery(invalidURLQuery)))
  })

  dispatch(templateVariablesSelectedByName(dashboardID, urlQueries))
}

const syncDashboardTimeRangeFromURLQueries = (
  dashboardID,
  urlQueries,
  location
) => (dispatch, getState) => {
  const dashboard = getState().dashboardUI.dashboards.find(
    d => d.id === dashboardID
  )

  const {
    upper = null,
    lower = null,
    zoomedUpper = null,
    zoomedLower = null,
  } = urlQueries

  let timeRange
  const {dashTimeV1} = getState()

  const timeRangeFromQueries = {
    upper: urlQueries.upper,
    lower: urlQueries.lower,
  }
  const timeRangeOrNull = validTimeRange(timeRangeFromQueries)

  if (timeRangeOrNull) {
    timeRange = timeRangeOrNull
  } else {
    const dashboardTimeRange = dashTimeV1.ranges.find(
      r => r.dashboardID === idNormalizer(TYPE_ID, dashboardID)
    )

    timeRange = dashboardTimeRange || defaultTimeRange

    if (timeRangeFromQueries.lower || timeRangeFromQueries.upper) {
      dispatch(
        notify(notifyInvalidTimeRangeValueInURLQuery(timeRangeFromQueries))
      )
    }
  }

  dispatch(setDashTimeV1(dashboardID, timeRange))

  if (!validAbsoluteTimeRange({lower: zoomedLower, upper: zoomedUpper})) {
    if (zoomedLower || zoomedUpper) {
      dispatch(notify(notifyInvalidZoomedTimeRangeValueInURLQuery()))
    }
  }

  dispatch(setZoomedTimeRange({zoomedLower, zoomedUpper}))

  const urlQueryTimeRanges = {
    upper,
    lower,
    zoomedUpper,
    zoomedLower,
  }
  dispatch(
    syncURLQueryFromTempVars(
      location,
      dashboard.templates,
      [],
      urlQueryTimeRanges
    )
  )
}

const syncDashboardFromURLQueries = (dashboardID, location) => dispatch => {
  const urlQueries = queryString.parse(window.location.search)

  dispatch(syncDashboardTempVarsFromURLQueries(dashboardID, urlQueries))
  dispatch(
    syncDashboardTimeRangeFromURLQueries(dashboardID, urlQueries, location)
  )
}

export const getDashboardWithHydratedAndSyncedTempVarsAsync = (
  dashboardID,
  source,
  router,
  location
) => async dispatch => {
  const dashboard = await bindActionCreators(getDashboardAsync, dispatch)(
    dashboardID,
    source,
    router
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

  dispatch(syncDashboardFromURLQueries(+dashboardID, location))
}

export const setZoomedTimeRangeAsync = (
  zoomedTimeRange,
  location
) => async dispatch => {
  dispatch(setZoomedTimeRange(zoomedTimeRange))
  dispatch(syncURLQueryFromQueriesObject(location, zoomedTimeRange))
}
