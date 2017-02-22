import {
  getDashboards as getDashboardsAPI,
  updateDashboard as updateDashboardAPI,
} from 'src/dashboards/apis'

export function loadDashboards(dashboards, dashboardID) {
  return {
    type: 'LOAD_DASHBOARDS',
    payload: {
      dashboards,
      dashboardID,
    },
  }
}

export function setDashboard(dashboardID) {
  return {
    type: 'SET_DASHBOARD',
    payload: {
      dashboardID,
    },
  }
}

export function setTimeRange(timeRange) {
  return {
    type: 'SET_DASHBOARD_TIME_RANGE',
    payload: {
      timeRange,
    },
  }
}

export function setEditMode(isEditMode) {
  return {
    type: 'SET_EDIT_MODE',
    payload: {
      isEditMode,
    },
  }
}

export function getDashboards(dashboardID) {
  return (dispatch) => {
    getDashboardsAPI().then(({data: {dashboards}}) => {
      dispatch(loadDashboards(dashboards, dashboardID))
    });
  }
}

export function putDashboard(dashboard) {
  return (dispatch) => {
    updateDashboardAPI(dashboard).then(({data}) => {
      dispatch(updateDashboard(data))
    })
  }
}

export function updateDashboard(dashboard) {
  return {
    type: 'UPDATE_DASHBOARD',
    payload: {
      dashboard,
    },
  }
}
