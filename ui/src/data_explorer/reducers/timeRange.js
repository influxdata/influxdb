import timeRanges from 'hson!../../shared/data/timeRanges.hson'

const {lower, upper} = timeRanges[2]

const initialState = {
  upper,
  lower,
}

export default function timeRange(state = initialState, action) {
  switch (action.type) {
    case 'SET_TIME_RANGE': {
      const {bounds} = action.payload

      return {...state, ...bounds}
    }
  }
  return state
}
