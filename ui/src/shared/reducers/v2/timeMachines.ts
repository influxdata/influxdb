// Libraries
import _ from 'lodash'

// Utils
import {convertView, createView, replaceQuery} from 'src/shared/utils/view'

// Constants
import {
  VEO_TIME_MACHINE_ID,
  DE_TIME_MACHINE_ID,
} from 'src/shared/constants/timeMachine'

// Types
import {TimeRange} from 'src/types/v2'
import {NewView} from 'src/types/v2/dashboards'
import {Action} from 'src/shared/actions/v2/timeMachines'

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
      const {view, draftScript} = activeTimeMachine

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: replaceQuery(view, draftScript),
      }
      break
    }
    case 'SET_AXES': {
      const {axes} = action.payload
      const {
        view,
        view: {properties},
      } = activeTimeMachine

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...properties, axes}},
      }
      break
    }

    case 'SET_Y_AXIS_LABEL': {
      const {label} = action.payload
      const {
        view,
        view: {properties},
      } = activeTimeMachine

      const axes = _.get(properties, 'axes')
      const yAxis = {..._.get(axes, 'y'), label}

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...properties, axes: {...axes, y: yAxis}}},
      }
      break
    }

    case 'SET_Y_AXIS_MIN_BOUND': {
      const {min} = action.payload
      const {
        view,
        view: {properties},
      } = activeTimeMachine

      const axes = _.get(properties, 'axes')
      const yAxis = _.get(axes, 'y')
      yAxis.bounds[0] = min

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...properties, axes: {...axes, y: yAxis}}},
      }
      break
    }

    case 'SET_Y_AXIS_MAX_BOUND': {
      const {max} = action.payload
      const {
        view,
        view: {properties},
      } = activeTimeMachine

      const axes = _.get(properties, 'axes')
      const yAxis = _.get(axes, 'y')
      yAxis.bounds[1] = max

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...properties, axes: {...axes, y: yAxis}}},
      }
      break
    }

    case 'SET_Y_AXIS_PREFIX': {
      const {prefix} = action.payload
      const {
        view,
        view: {properties},
      } = activeTimeMachine

      const axes = _.get(properties, 'axes')
      const yAxis = {..._.get(axes, 'y'), prefix}

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...properties, axes: {...axes, y: yAxis}}},
      }
      break
    }

    case 'SET_Y_AXIS_SUFFIX': {
      const {suffix} = action.payload
      const {
        view,
        view: {properties},
      } = activeTimeMachine

      const axes = _.get(properties, 'axes')
      const yAxis = {..._.get(axes, 'y'), suffix}

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...properties, axes: {...axes, y: yAxis}}},
      }
      break
    }

    case 'SET_Y_AXIS_BASE': {
      const {base} = action.payload
      const {
        view,
        view: {properties},
      } = activeTimeMachine

      const axes = _.get(properties, 'axes')
      const yAxis = {..._.get(axes, 'y'), base}

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...properties, axes: {...axes, y: yAxis}}},
      }
      break
    }

    case 'SET_Y_AXIS_SCALE': {
      const {scale} = action.payload
      const {
        view,
        view: {properties},
      } = activeTimeMachine

      const axes = _.get(properties, 'axes')
      const yAxis = {..._.get(axes, 'y'), scale}

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...properties, axes: {...axes, y: yAxis}}},
      }
      break
    }

    case 'SET_COLORS': {
      const {colors} = action.payload
      const {
        view,
        view: {properties},
      } = activeTimeMachine

      newActiveTimeMachine = {
        ...activeTimeMachine,
        view: {...view, properties: {...properties, colors}},
      }
      break
    }

    case 'SET_DECIMAL_PLACES': {
      const {decimalPlaces} = action.payload

      newActiveTimeMachine = {...activeTimeMachine, decimalPlaces}
      break
    }

    case 'SET_STATIC_LEGEND': {
      const {staticLegend} = action.payload

      newActiveTimeMachine = {...activeTimeMachine, staticLegend}
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
