import {
  getDashboards as getDashboardsAJAX,
  updateDashboard as updateDashboardAJAX,
  deleteDashboard as deleteDashboardAJAX,
  updateDashboardCell as updateDashboardCellAJAX,
  addDashboardCell as addDashboardCellAJAX,
  deleteDashboardCell as deleteDashboardCellAJAX,
} from 'src/dashboards/apis'

import {publishNotification} from 'src/shared/actions/notifications';

import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants'

export const loadDashboards = (dashboards, dashboardID) => ({
  type: 'LOAD_DASHBOARDS',
  payload: {
    dashboards,
    dashboardID,
  },
})

export const setDashboard = (dashboardID) => ({
  type: 'SET_DASHBOARD',
  payload: {
    dashboardID,
  },
})

export const setTimeRange = (timeRange) => ({
  type: 'SET_DASHBOARD_TIME_RANGE',
  payload: {
    timeRange,
  },
})

export const updateDashboard = (dashboard) => ({
  type: 'UPDATE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const deleteDashboard = (dashboard) => ({
  type: 'DELETE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const undoDeleteDashboard = (dashboard) => ({
  type: 'UNDO_DELETE_DASHBOARD',
  payload: {
    dashboard,
  },
})

export const updateDashboardCells = (cells) => ({
  type: 'UPDATE_DASHBOARD_CELLS',
  payload: {
    cells,
  },
})

export const syncDashboardCell = (cell) => ({
  type: 'SYNC_DASHBOARD_CELL',
  payload: {
    cell,
  },
})

export const addDashboardCell = (cell) => ({
  type: 'ADD_DASHBOARD_CELL',
  payload: {
    cell,
  },
})

export const editDashboardCell = (x, y, isEditing) => ({
  type: 'EDIT_DASHBOARD_CELL',
  // x and y coords are used as a alternative to cell ids, which are not
  // universally unique, and cannot be because React depends on a
  // quasi-predictable ID for keys. Since cells cannot overlap, coordinates act
  // as a suitable id
  payload: {
    x,  // x-coord of the cell to be edited
    y,  // y-coord of the cell to be edited
    isEditing,
  },
})

export const renameDashboardCell = (x, y, name) => ({
  type: 'RENAME_DASHBOARD_CELL',
  payload: {
    x,  // x-coord of the cell to be renamed
    y,  // y-coord of the cell to be renamed
    name,
  },
})

export const deleteDashboardCell = (cell) => ({
  type: 'DELETE_DASHBOARD_CELL',
  payload: {
    cell,
  },
})

// Async Action Creators

export const getDashboardsAsync = (dashboardID) => async (dispatch) => {
  const {data: {dashboards}} = await getDashboardsAJAX()
  dispatch(loadDashboards(dashboards, dashboardID))
}

export const putDashboard = () => (dispatch, getState) => {
  const {dashboardUI: {dashboard}} = getState()
  updateDashboardAJAX(dashboard).then(({data}) => {
    dispatch(updateDashboard(data))
  })
}

export const updateDashboardCell = (cell) => (dispatch) => {
  return updateDashboardCellAJAX(cell)
  .then(({data}) => {
    dispatch(syncDashboardCell(data))
  })
}

export const deleteDashboardAsync = (dashboard) => async (dispatch) => {
  dispatch(deleteDashboard(dashboard))
  try {
    await deleteDashboardAJAX(dashboard)
    dispatch(publishNotification('success', 'Dashboard deleted successfully.'))
  } catch (error) {
    dispatch(undoDeleteDashboard(dashboard)) // undo optimistic update
    dispatch(publishNotification('error', `Failed to delete dashboard: ${error.data.message}.`))
  }
}

export const addDashboardCellAsync = (dashboard) => async (dispatch) => {
  try {
    const {data} = await addDashboardCellAJAX(dashboard, NEW_DEFAULT_DASHBOARD_CELL)
    dispatch(addDashboardCell(data))
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const deleteDashboardCellAsync = (cell) => async (dispatch) => {
  try {
    await deleteDashboardCellAJAX(cell)
    dispatch(deleteDashboardCell(cell))
  } catch (error) {
    console.error(error)
    throw error
  }
}
