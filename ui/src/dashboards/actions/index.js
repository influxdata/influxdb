import {
  getDashboards as getDashboardsAJAX,
  updateDashboard as updateDashboardAJAX,
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


// Async Action Creators

export const getDashboards = (dashboardID) => (dispatch) => {
  getDashboardsAJAX().then(({data: {dashboards}}) => {
    dispatch(loadDashboards(dashboards, dashboardID))
  })
}

export const putDashboard = (dashboard) => (dispatch) => {
  updateDashboardAJAX(dashboard).then(({data}) => {
    dispatch(updateDashboard(data))
  })
}
