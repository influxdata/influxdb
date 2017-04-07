import timeRanges from 'hson!../../shared/data/timeRanges.hson'

const initialLower = timeRanges[1].lower
const initialUpper = timeRanges[1].upper

const initialState = {
  upper: initialUpper,
  lower: initialLower,
}

export default function timeRange(state = initialState, action) {
  switch (action.type) {
    case 'SET_TIME_RANGE': {
      const {upper, lower} = action.payload
      const newState = {
        upper,
        lower,
      }

      return {...state, ...newState}
    }
  }
  return state
}
