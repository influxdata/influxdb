import {AUTOREFRESH_DEFAULT} from 'shared/constants'
import timeRanges from 'hson!shared/data/timeRanges.hson'

import * as actionTypes from 'src/status/constants/actionTypes'

const {lower, upper} = timeRanges.find(tr => tr.lower === 'now() - 30d')

const initialState = {
  autoRefresh: AUTOREFRESH_DEFAULT,
  timeRange: {lower, upper},
}

const statusUI = (state = initialState, action) => {
  switch (action.type) {
    case actionTypes.SET_STATUS_PAGE_AUTOREFRESH: {
      const {milliseconds} = action.payload

      return {
        ...state,
        autoRefresh: milliseconds,
      }
    }

    case actionTypes.SET_STATUS_PAGE_TIME_RANGE: {
      const {timeRange} = action.payload

      return {...state, timeRange}
    }

    default: {
      return state
    }
  }
}

export default statusUI
