import {timeRanges} from 'src/shared/data/timeRanges'
import {TimeRange} from 'src/types'

const {lower, upper} = timeRanges.find(tr => tr.lower === 'now() - 1h')

const initialState = {
  upper,
  lower,
}

type State = Readonly<TimeRange>

interface ActionSetTimeRange {
  type: 'DE_SET_TIME_RANGE'
  payload: {
    bounds: TimeRange
  }
}

type Action = ActionSetTimeRange

const timeRange = (state: State = initialState, action: Action): State => {
  switch (action.type) {
    case 'DE_SET_TIME_RANGE': {
      const {bounds} = action.payload

      return {...state, ...bounds}
    }
  }
  return state
}

export default timeRange
