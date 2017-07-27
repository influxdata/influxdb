import timeRanges from 'hson!shared/data/timeRanges.hson'

const {lower, upper} = timeRanges.find(tr => tr.lower === 'now() - 1h')

const initialState = {
  upper,
  lower,
}

export default function timeRange(state = initialState, action) {
  switch (action.type) {
    case 'DE_SET_TIME_RANGE': {
      const {bounds} = action.payload

      return {...state, ...bounds}
    }
  }
  return state
}
