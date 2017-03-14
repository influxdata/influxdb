import _ from 'lodash';
import {EMPTY_DASHBOARD} from 'src/dashboards/constants'
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

const initialState = {
  dashboards: [],
  dashboard: EMPTY_DASHBOARD,
  timeRange: timeRanges[1],
  isEditMode: false,
};

export default function ui(state = initialState, action) {
  switch (action.type) {
    case 'LOAD_DASHBOARDS': {
      const {dashboards, dashboardID} = action.payload;
      const newState = {
        dashboards,
        dashboard: _.find(dashboards, (d) => d.id === +dashboardID),
      };

      return {...state, ...newState};
    }

    case 'SET_DASHBOARD': {
      const {dashboardID} = action.payload
      const newState = {
        dashboard: _.find(state.dashboards, (d) => d.id === +dashboardID),
      };

      return {...state, ...newState}
    }

    case 'SET_DASHBOARD_TIME_RANGE': {
      const {timeRange} = action.payload

      return {...state, timeRange};
    }

    case 'SET_EDIT_MODE': {
      const {isEditMode} = action.payload
      return {...state, isEditMode}
    }

    case 'UPDATE_DASHBOARD': {
      const {dashboard} = action.payload
      const newState = {
        dashboard,
        dashboards: state.dashboards.map((d) => d.id === dashboard.id ? dashboard : d),
      }

      return {...state, ...newState}
    }

    case 'UPDATE_DASHBOARD_CELLS': {
      const {cells} = action.payload
      const {dashboard} = state

      const newDashboard = {
        ...dashboard,
        cells,
      }

      const newState = {
        dashboard: newDashboard,
        dashboards: state.dashboards.map((d) => d.id === dashboard.id ? newDashboard : d),
      }

      return {...state, ...newState}
    }

    case 'EDIT_CELL': {
      const {x, y, isEditing} = action.payload
      const {dashboard} = state

      const cell = dashboard.cells.find((c) => c.x === x && c.y === y)

      const newCell = {
        ...cell,
        isEditing,
      }

      const newDashboard = {
        ...dashboard,
        cells: dashboard.cells.map((c) => c.x === x && c.y === y ? newCell : c),
      }

      const newState = {
        dashboard: newDashboard,
        dashboards: state.dashboards.map((d) => d.id === dashboard.id ? newDashboard : d),
      }

      return {...state, ...newState}
    }

    case 'RENAME_CELL': {
      const {x, y, name} = action.payload
      const {dashboard} = state

      const cell = dashboard.cells.find((c) => c.x === x && c.y === y)

      const newCell = {
        ...cell,
        name,
      }

      const newDashboard = {
        ...dashboard,
        cells: dashboard.cells.map((c) => c.x === x && c.y === y ? newCell : c),
      }

      const newState = {
        dashboard: newDashboard,
        dashboards: state.dashboards.map((d) => d.id === dashboard.id ? newDashboard : d),
      }

      return {...state, ...newState}
    }
  }

  return state;
}
