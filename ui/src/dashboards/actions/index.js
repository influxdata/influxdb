import {
  getDashboards as getDashboardsAJAX,
  updateDashboard as updateDashboardAJAX,
  updateDashboardCell as updateDashboardCellAJAX,
} from 'src/dashboards/apis'

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

export const setEditMode = (isEditMode) => ({
  type: 'SET_EDIT_MODE',
  payload: {
    isEditMode,
  },
})

export const updateDashboard = (dashboard) => ({
  type: 'UPDATE_DASHBOARD',
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

export const editCell = (x, y, isEditing) => ({
  type: 'EDIT_CELL',
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

export const renameCell = (x, y, name) => ({
  type: 'RENAME_CELL',
  payload: {
    x,  // x-coord of the cell to be renamed
    y,  // y-coord of the cell to be renamed
    name,
  },
})

// Async Action Creators

export const getDashboards = (dashboardID) => (dispatch) => {
  getDashboardsAJAX().then(({data: {dashboards}}) => {
    dispatch(loadDashboards(dashboards, dashboardID))
  })
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
