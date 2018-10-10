// Utils
import {getNewView} from 'src/dashboards/utils/cellGetters'

// Constants
import {
  VEO_TIME_MACHINE_ID,
  DE_TIME_MACHINE_ID,
} from 'src/shared/constants/timeMachine'

// Types
import {View, TimeRange} from 'src/types/v2'
import {Action} from 'src/shared/actions/v2/timeMachines'

interface TimeMachineState {
  view: Partial<View>
  timeRange: TimeRange
}

export interface TimeMachinesState {
  activeTimeMachineID: string
  timeMachines: {
    [timeMachineID: string]: TimeMachineState
  }
}

const initialStateHelper = (): TimeMachineState => ({
  view: getNewView(),
  timeRange: {lower: 'now() - 1h'},
})

const INITIAL_STATE: TimeMachinesState = {
  activeTimeMachineID: DE_TIME_MACHINE_ID,
  timeMachines: {
    [VEO_TIME_MACHINE_ID]: initialStateHelper(),
    [DE_TIME_MACHINE_ID]: initialStateHelper(),
  },
}

const timeMachineReducer = (
  state = INITIAL_STATE,
  action: Action
): TimeMachinesState => {
  if (action.type === 'SET_ACTIVE_TIME_MACHINE_ID') {
    return {...state, activeTimeMachineID: action.payload.activeTimeMachineID}
  }

  // All other actions act upon whichever single `TimeMachineState` is
  // specified by the `activeTimeMachineID` property

  const {activeTimeMachineID, timeMachines} = state
  const activeTimeMachine = timeMachines[activeTimeMachineID]

  if (!activeTimeMachine) {
    return state
  }

  let newActiveTimeMachine

  switch (action.type) {
    case 'SET_VIEW_NAME': {
      const {name} = action.payload
      const view = {...activeTimeMachine.view, name}

      newActiveTimeMachine = {...activeTimeMachine, view}
      break
    }
    case 'SET_TIME_RANGE': {
      const {timeRange} = action.payload

      newActiveTimeMachine = {...activeTimeMachine, timeRange}
      break
    }
  }

  if (newActiveTimeMachine) {
    return {
      ...state,
      timeMachines: {
        ...timeMachines,
        [activeTimeMachineID]: newActiveTimeMachine,
      },
    }
  }

  return state
}

export default timeMachineReducer
