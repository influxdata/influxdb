import {getDashboards as getDashboardsAPI} from 'src/dashboards/apis'

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

export function getDashboards(dashboardID) {
  return (dispatch) => {
    getDashboardsAPI().then(({data: {dashboards}}) => {
      dispatch(loadDashboards(dashboards, dashboardID))
    });
  }
}
