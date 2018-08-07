import _ from 'lodash'

import {TimeRange} from 'src/types'
import {Action, ActionTypes} from 'src/dashboards/actions/v2/ranges'

interface Range extends TimeRange {
  dashboardID: string
}

type State = Range[]

const initialState: State = []

export default (state: State = initialState, action: Action) => {
  switch (action.type) {
    case ActionTypes.DeleteTimeRange: {
      const {dashboardID} = action.payload
      const ranges = state.filter(r => r.dashboardID !== dashboardID)

      return ranges
    }

    case ActionTypes.RetainRangesDashboardTimeV1: {
      const {dashboardIDs} = action.payload
      const ranges = state.filter(r => dashboardIDs.includes(r.dashboardID))
      return ranges
    }

    case ActionTypes.SetDashboardTimeV1: {
      const {dashboardID, timeRange} = action.payload
      const newTimeRange = [{dashboardID, ...timeRange}]
      const ranges = _.unionBy(newTimeRange, state, 'dashboardID')

      return ranges
    }
  }

  return state
}
