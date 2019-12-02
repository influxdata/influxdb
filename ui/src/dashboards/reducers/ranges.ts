import {TimeRange} from 'src/types'
import {Action, ActionTypes} from 'src/dashboards/actions/ranges'

export type RangeState = {
  [contextID: string]: TimeRange
}

const initialState: RangeState = {}

export default (
  state: RangeState = initialState,
  action: Action
): RangeState => {
  switch (action.type) {
    case ActionTypes.DeleteTimeRange: {
      const {dashboardID} = action.payload
      const {[dashboardID]: _, ...filteredRanges} = state

      return filteredRanges
    }

    case ActionTypes.RetainRangesDashboardTimeV1: {
      const {dashboardIDs} = action.payload
      const ranges = {}
      for (const key in state) {
        if (dashboardIDs.includes(key)) {
          ranges[key] = state[key]
        }
      }
      return ranges
    }

    case ActionTypes.SetDashboardTimeRange: {
      const {dashboardID, timeRange} = action.payload
      return {...state, [dashboardID]: timeRange}
    }
  }

  return state
}
