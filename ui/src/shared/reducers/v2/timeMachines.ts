// Utils
import {convertView} from 'src/shared/utils/view'

// Constants
import {
  VEO_TIME_MACHINE_ID,
  DE_TIME_MACHINE_ID,
} from 'src/shared/constants/timeMachine'

// Types
import {View, TimeRange, ViewType, ViewShape} from 'src/types/v2'
import {Action} from 'src/shared/actions/v2/timeMachines'
import {InfluxLanguages} from 'src/types/v2/dashboards'

interface TimeMachineState {
  view: View
  timeRange: TimeRange
}

export interface TimeMachinesState {
  activeTimeMachineID: string
  timeMachines: {
    [timeMachineID: string]: TimeMachineState
  }
}

const initialStateHelper = (): TimeMachineState => ({
  timeRange: {lower: 'now() - 1h'},
  view: {
    id: '1',
    name: 'CELLL YO',
    properties: {
      shape: ViewShape.ChronografV2,
      queries: [
        {
          text:
            'SELECT mean("usage_user") FROM "telegraf"."autogen"."cpu" WHERE time > now() - 15m GROUP BY time(10s) FILL(0)',
          type: InfluxLanguages.InfluxQL,
          source: 'v1',
        },
      ],
      axes: {
        x: {
          bounds: ['', ''],
          label: '',
          prefix: '',
          suffix: '',
          base: '10',
          scale: 'linear',
        },
        y: {
          bounds: ['', ''],
          label: '',
          prefix: '',
          suffix: '',
          base: '10',
          scale: 'linear',
        },
        y2: {
          bounds: ['', ''],
          label: '',
          prefix: '',
          suffix: '',
          base: '10',
          scale: 'linear',
        },
      },
      type: ViewType.Line,
      colors: [
        {
          id: '63b61e02-7649-4d88-84bd-97722e2a2514',
          type: 'scale',
          hex: '#31C0F6',
          name: 'Nineteen Eighty Four',
          value: '0',
        },
        {
          id: 'd77c12d4-d257-48e1-8ba5-7bee8e3df593',
          type: 'scale',
          hex: '#A500A5',
          name: 'Nineteen Eighty Four',
          value: '0',
        },
        {
          id: 'cd6948ad-7ae6-40d3-bc37-3aec32f7fe98',
          type: 'scale',
          hex: '#FF7E27',
          name: 'Nineteen Eighty Four',
          value: '0',
        },
      ],
      legend: {},
      decimalPlaces: {
        isEnforced: false,
        digits: 3,
      },
    },
  },
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

    case 'SET_VIEW_TYPE': {
      const {type} = action.payload
      const view = convertView(activeTimeMachine.view, type)

      newActiveTimeMachine = {...activeTimeMachine, view}
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
