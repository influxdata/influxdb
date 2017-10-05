const initialState = {
  dashTimeV1: [],
}

const dashTimeV1 = (state = initialState, action) => {
  switch (action.type) {
    case 'SET_DASHBOARD_TIME_RANGE_V1': {
      const {dashboardID, timeRange} = action.payload
      const newState = {
        dashTimeV1: [...state.dashTimeV1, {dashboardID, timeRange}],
      }

      return {...state, ...newState}
    }
  }

  return state
}

export default dashTimeV1
