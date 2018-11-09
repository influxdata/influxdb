// Utils
import {convertView, createView} from 'src/shared/utils/view'

// Constants
import {
  VEO_TIME_MACHINE_ID,
  DE_TIME_MACHINE_ID,
} from 'src/shared/constants/timeMachine'

// Types
import {TimeRange} from 'src/types/v2'
import {NewView} from 'src/types/v2/dashboards'
import {Action} from 'src/shared/actions/v2/timeMachines'
import {InfluxLanguage} from 'src/types/v2/dashboards'

export interface TimeMachineState {
  view: NewView
  timeRange: TimeRange
  draftScript: string
}

export interface TimeMachinesState {
  activeTimeMachineID: string
  timeMachines: {
    [timeMachineID: string]: TimeMachineState
  }
}

const initialStateHelper = (): TimeMachineState => ({
  timeRange: {lower: 'now() - 1h'},
  view: createView(),
  draftScript: '',
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
  if (action.type === 'SET_ACTIVE_TIME_MACHINE') {
    const {activeTimeMachineID, initialState} = action.payload

    return {
      ...state,
      activeTimeMachineID,
      timeMachines: {
        ...state.timeMachines,
        [activeTimeMachineID]: {
          ...state.timeMachines[activeTimeMachineID],
          ...initialState,
        },
      },
    }
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

    case 'SET_VIEW_TYPE': {
      const {type} = action.payload
      const view = convertView(activeTimeMachine.view, type)

      newActiveTimeMachine = {...activeTimeMachine, view}
      break
    }

    case 'SET_DRAFT_SCRIPT': {
      const {draftScript} = action.payload

      newActiveTimeMachine = {...activeTimeMachine, draftScript}
      break
    }

    case 'SUBMIT_SCRIPT': {
      const view: any = activeTimeMachine.view

      if (!view.properties.queries) {
        break
      }

      const queries = [
        {
          type: InfluxLanguage.Flux,
          text: activeTimeMachine.draftScript,
          source: '',
        },
      ]

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...view.properties, queries}},
      }
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
