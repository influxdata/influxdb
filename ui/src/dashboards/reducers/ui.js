import _ from 'lodash';
import {EMPTY_DASHBOARD} from 'src/dashboards/constants'
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

const initialState = {
  dashboards: [],
  dashboard: EMPTY_DASHBOARD,
  timeRange: timeRanges[1],
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
  }

  return state;
}
