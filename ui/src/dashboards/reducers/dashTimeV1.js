const initialState = {
  ranges: [],
}

const dashTimeV1 = (state = initialState, action) => {
  switch (action.type) {
    case 'ADD_DASHBOARD_TIME_V1': {
      const {dashboardID, timeRange} = action.payload
      const ranges = [...state.ranges, {dashboardID, timeRange}]

      return {...state, ranges}
    }

    case 'DELETE_DASHBOARD': {
      const {dashboardID} = action.payload
      const ranges = state.ranges.filter(d => d.id !== dashboardID)

      return {...state, ranges}
    }

    case 'UPDATE_DASHBOARD_TIME_V1': {
      const {dashboardID, timeRange} = action.payload
      const exists = state.ranges.find(r => r.dashboardID === dashboardID)
      const {upper, lower} = timeRange

      if (!exists) {
        return {
          ...state,
          ranges: [...state.ranges, {dashboardID, upper, lower}],
        }
      }

      const ranges = state.ranges.map(
        d => (d.dashboardID === dashboardID ? {dashboardID, upper, lower} : d)
      )

      return {...state, ranges}
    }
  }

  return state
}

export default dashTimeV1
