import {AUTOREFRESH_DEFAULT} from 'shared/constants'
import timeRanges from 'hson!shared/data/timeRanges.hson'

const {lower, upper} = timeRanges[2]

const initialState = {
  autoRefresh: AUTOREFRESH_DEFAULT,
  timeRange: {lower, upper},
}

const status = (state = initialState, action) => {
  switch (action.type) {
    case 'SET_STATUS_PAGE_AUTOREFRESH': {
      const {milliseconds} = action.payload

      return {
        ...state,
        autoRefresh: milliseconds,
      }
    }

    case 'SET_STATUS_PAGE_TIME_RANGE': {
      const {timeRange} = action.payload

      return {...state, timeRange}
    }

    default: {
      return state
    }
  }
}

export default status
