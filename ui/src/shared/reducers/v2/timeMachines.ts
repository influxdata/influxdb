// Libraries
import {get} from 'lodash'

// Utils
import {convertView, createView, replaceQuery} from 'src/shared/utils/view'

// Constants
import {
  VEO_TIME_MACHINE_ID,
  DE_TIME_MACHINE_ID,
} from 'src/shared/constants/timeMachine'

// Types
import {TimeRange} from 'src/types/v2'
import {
  View,
  NewView,
  QueryViewProperties,
  DashboardQuery,
} from 'src/types/v2/dashboards'
import {Action} from 'src/shared/actions/v2/timeMachines'
import {TimeMachineTab} from 'src/types/v2/timeMachine'

export interface TimeMachineState {
  view: View<QueryViewProperties> | NewView<QueryViewProperties>
  timeRange: TimeRange
  draftScript: string
  isViewingRawData: boolean
  activeTab: TimeMachineTab
  activeQueryIndex: number | null
  submitToken: number
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
  isViewingRawData: false,
  activeTab: TimeMachineTab.Queries,
  activeQueryIndex: 0,
  submitToken: 0,
})

export const initialState = (): TimeMachinesState => ({
  activeTimeMachineID: DE_TIME_MACHINE_ID,
  timeMachines: {
    [VEO_TIME_MACHINE_ID]: initialStateHelper(),
    [DE_TIME_MACHINE_ID]: initialStateHelper(),
  },
})

const timeMachinesReducer = (
  state: TimeMachinesState = initialState(),
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

  const newActiveTimeMachine = timeMachineReducer(activeTimeMachine, action)

  return {
    ...state,
    timeMachines: {
      ...timeMachines,
      [activeTimeMachineID]: newActiveTimeMachine,
    },
  }
}

const setViewProperties = (
  state: TimeMachineState,
  update: {[key: string]: any}
): TimeMachineState => {
  const view: any = state.view
  const properties = view.properties

  return {...state, view: {...view, properties: {...properties, ...update}}}
}

const setYAxis = (state: TimeMachineState, update: {[key: string]: any}) => {
  const view: any = state.view
  const properties = view.properties
  const axes = get(properties, 'axes', {})
  const yAxis = get(axes, 'y', {})

  return {
    ...state,
    view: {
      ...view,
      properties: {...properties, axes: {...axes, y: {...yAxis, ...update}}},
    },
  }
}

const timeMachineReducer = (
  state: TimeMachineState,
  action: Action
): TimeMachineState => {
  switch (action.type) {
    case 'SET_VIEW_NAME': {
      const {name} = action.payload
      const view = {...state.view, name}

      return {...state, view}
    }

    case 'SET_TIME_RANGE': {
      const {timeRange} = action.payload

      return {...state, timeRange}
    }

    case 'SET_VIEW_TYPE': {
      const {type} = action.payload
      const view = convertView(state.view, type)

      return {...state, view}
    }

    case 'SET_DRAFT_SCRIPT': {
      const {draftScript} = action.payload

      return {...state, draftScript}
    }

    case 'SUBMIT_SCRIPT': {
      const {view, draftScript} = state

      return {
        ...state,
        view: replaceQuery(view, draftScript),
        submitToken: Date.now(),
      }
    }

    case 'SET_IS_VIEWING_RAW_DATA': {
      const {isViewingRawData} = action.payload

      return {...state, isViewingRawData}
    }

    case 'SET_ACTIVE_TAB': {
      const {activeTab} = action.payload
      return {...state, activeTab}
    }

    case 'SET_AXES': {
      const {axes} = action.payload

      return setViewProperties(state, {axes})
    }

    case 'SET_GEOM': {
      const {geom} = action.payload

      return setViewProperties(state, {geom})
    }

    case 'SET_Y_AXIS_LABEL': {
      const {label} = action.payload

      return setYAxis(state, {label})
    }

    case 'SET_Y_AXIS_MIN_BOUND': {
      const {min} = action.payload

      const bounds = [...get(state, 'view.properties.axes.y.bounds', [])]

      bounds[0] = min

      return setYAxis(state, {bounds})
    }

    case 'SET_Y_AXIS_MAX_BOUND': {
      const {max} = action.payload

      const bounds = [...get(state, 'view.properties.axes.y.bounds', [])]

      bounds[1] = max

      return setYAxis(state, {bounds})
    }

    case 'SET_Y_AXIS_PREFIX': {
      const {prefix} = action.payload

      return setYAxis(state, {prefix})
    }

    case 'SET_Y_AXIS_SUFFIX': {
      const {suffix} = action.payload

      return setYAxis(state, {suffix})
    }

    case 'SET_Y_AXIS_BASE': {
      const {base} = action.payload

      return setYAxis(state, {base})
    }

    case 'SET_Y_AXIS_SCALE': {
      const {scale} = action.payload

      return setYAxis(state, {scale})
    }

    case 'SET_PREFIX': {
      const {prefix} = action.payload

      return setYAxis(state, {prefix})
    }

    case 'SET_SUFFIX': {
      const {suffix} = action.payload

      return setYAxis(state, {suffix})
    }

    case 'SET_COLORS': {
      const {colors} = action.payload

      return setViewProperties(state, {colors})
    }

    case 'SET_DECIMAL_PLACES': {
      const {decimalPlaces} = action.payload

      return setViewProperties(state, {decimalPlaces})
    }

    case 'SET_STATIC_LEGEND': {
      const {staticLegend} = action.payload

      return setViewProperties(state, {staticLegend})
    }

    case 'SET_QUERY_SOURCE': {
      const {sourceID} = action.payload
      const {activeQueryIndex} = state
      const selectedQuery: string | undefined = get(
        state,
        `view.properties.queries.${activeQueryIndex}`
      )

      if (!selectedQuery) {
        return state
      }

      const queries = state.view.properties.queries.map(
        (query: DashboardQuery, i) => {
          if (i !== activeQueryIndex) {
            return query
          }

          return {...query, sourceID}
        }
      )

      return setViewProperties(state, {queries})
    }

    case 'INCREMENT_SUBMIT_TOKEN': {
      return {
        ...state,
        submitToken: Date.now(),
      }
    }
  }

  return state
}

export default timeMachinesReducer
