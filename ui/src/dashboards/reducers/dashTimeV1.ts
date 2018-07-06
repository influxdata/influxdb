import _ from 'lodash'

import {TimeRange} from 'src/types'
import {Action, ActionType} from 'src/dashboards/actions'

interface Range extends TimeRange {
  dashboardID: number
}

interface State {
  ranges: Range[]
}

const initialState: State = {
  ranges: [],
}

export default (state: State = initialState, action: Action) => {
  switch (action.type) {
    case ActionType.DeleteDashboard: {
      const {dashboard} = action.payload
      const ranges = state.ranges.filter(r => r.dashboardID !== dashboard.id)

      return {...state, ranges}
    }

    case ActionType.RetainRangesDashboardTimeV1: {
      const {dashboardIDs} = action.payload
      const ranges = state.ranges.filter(r =>
        dashboardIDs.includes(r.dashboardID)
      )
      return {...state, ranges}
    }

    case ActionType.SetDashboardTimeV1: {
      const {dashboardID, timeRange} = action.payload
      const newTimeRange = [{dashboardID, ...timeRange}]
      const ranges = _.unionBy(newTimeRange, state.ranges, 'dashboardID')

      return {...state, ranges}
    }
  }

  return state
}
