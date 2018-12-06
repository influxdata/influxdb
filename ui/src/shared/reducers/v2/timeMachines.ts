// Libraries
import {get, cloneDeep} from 'lodash'

// Utils
import {createView, defaultViewQuery} from 'src/shared/utils/view'
import {isConfigValid, buildQuery} from 'src/shared/utils/queryBuilder'

// Constants
import {
  VEO_TIME_MACHINE_ID,
  DE_TIME_MACHINE_ID,
} from 'src/shared/constants/timeMachine'

// Types
import {TimeRange} from 'src/types/v2'
import {
  View,
  ViewType,
  NewView,
  QueryViewProperties,
  DashboardQuery,
  InfluxLanguage,
  QueryEditMode,
} from 'src/types/v2/dashboards'
import {Action} from 'src/shared/actions/v2/timeMachines'
import {TimeMachineTab} from 'src/types/v2/timeMachine'

export interface TimeMachineState {
  view: View<QueryViewProperties> | NewView<QueryViewProperties>
  timeRange: TimeRange
  draftQueries: DashboardQuery[]
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

export const initialStateHelper = (): TimeMachineState => ({
  timeRange: {lower: 'now() - 1h'},
  view: createView(),
  draftQueries: [defaultViewQuery()],
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

export const timeMachinesReducer = (
  state: TimeMachinesState = initialState(),
  action: Action
): TimeMachinesState => {
  if (action.type === 'SET_ACTIVE_TIME_MACHINE') {
    const {activeTimeMachineID, initialState} = action.payload
    const activeTimeMachine = state.timeMachines[activeTimeMachineID]
    const view = initialState.view || activeTimeMachine.view
    const draftQueries = cloneDeep(view.properties.queries)
    const activeQueryIndex = 0

    return {
      ...state,
      activeTimeMachineID,
      timeMachines: {
        ...state.timeMachines,
        [activeTimeMachineID]: {
          ...activeTimeMachine,
          ...initialState,
          activeTab: TimeMachineTab.Queries,
          activeQueryIndex,
          draftQueries,
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

export const timeMachineReducer = (
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

      // TODO(chnn): Rebuild the BuilderConfig for each query

      return {...state, timeRange}
    }

    case 'SET_VIEW_TYPE': {
      const {type} = action.payload
      const view = convertView(state.view, type)

      return {...state, view}
    }

    case 'SET_ACTIVE_QUERY_TEXT': {
      const {text} = action.payload
      const draftQueries = [...state.draftQueries]

      draftQueries[state.activeQueryIndex] = {
        ...draftQueries[state.activeQueryIndex],
        text,
      }

      return {...state, draftQueries}
    }

    case 'SUBMIT_SCRIPT': {
      const {view, draftQueries} = state

      return {
        ...state,
        submitToken: Date.now(),
        view: {
          ...view,
          properties: {
            ...view.properties,
            queries: draftQueries,
          },
        },
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

      const draftQueries = [...state.draftQueries]

      draftQueries[activeQueryIndex] = {
        ...draftQueries[activeQueryIndex],
        sourceID,
      }

      return {...state, draftQueries}
    }

    case 'INCREMENT_SUBMIT_TOKEN': {
      return {
        ...state,
        submitToken: Date.now(),
      }
    }

    case 'EDIT_ACTIVE_QUERY_WITH_BUILDER': {
      const {activeQueryIndex} = state
      const draftQueries = [...state.draftQueries]

      draftQueries[activeQueryIndex] = defaultViewQuery()

      return {
        ...state,
        draftQueries,
      }
    }

    case 'EDIT_ACTIVE_QUERY_AS_FLUX': {
      const {activeQueryIndex} = state
      const draftQueries = [...state.draftQueries]

      draftQueries[activeQueryIndex] = {
        ...draftQueries[activeQueryIndex],
        editMode: QueryEditMode.Advanced,
        type: InfluxLanguage.Flux,
      }

      return {
        ...state,
        draftQueries,
      }
    }

    case 'EDIT_ACTIVE_QUERY_AS_INFLUXQL': {
      const {activeQueryIndex} = state
      const draftQueries = [...state.draftQueries]

      draftQueries[activeQueryIndex] = {
        ...draftQueries[activeQueryIndex],
        editMode: QueryEditMode.Advanced,
        type: InfluxLanguage.InfluxQL,
      }

      return {
        ...state,
        draftQueries,
      }
    }

    case 'SET_ACTIVE_QUERY_INDEX': {
      const {activeQueryIndex} = action.payload

      return {...state, activeQueryIndex}
    }

    case 'ADD_QUERY': {
      const draftQueries = [...state.draftQueries, defaultViewQuery()]
      const activeQueryIndex: number = draftQueries.length - 1

      return {...state, activeQueryIndex, draftQueries}
    }

    case 'REMOVE_QUERY': {
      const {queryIndex} = action.payload
      const draftQueries = state.draftQueries.filter(
        (__, i) => i !== queryIndex
      )
      const queryLength = draftQueries.length

      let activeQueryIndex: number

      if (queryIndex < queryLength) {
        activeQueryIndex = queryIndex
      } else if (queryLength === queryIndex && queryLength > 0) {
        activeQueryIndex = queryLength - 1
      } else {
        activeQueryIndex = 0
      }

      return {...state, activeQueryIndex, draftQueries}
    }

    case 'BUILD_QUERY': {
      const {config} = action.payload
      const {activeQueryIndex, timeRange} = state
      const draftQueries = [...state.draftQueries]

      let text: string

      if (!isConfigValid(config)) {
        text = ''
      } else {
        text = buildQuery(config, timeRange.duration)
      }

      draftQueries[activeQueryIndex] = {
        ...draftQueries[activeQueryIndex],
        text,
      }

      return {...state, draftQueries}
    }
  }

  return state
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

const convertView = (
  view: View<QueryViewProperties> | NewView<QueryViewProperties>,
  outType: ViewType
): View<QueryViewProperties> => {
  const newView: any = createView(outType)

  newView.properties.queries = cloneDeep(view.properties.queries)
  newView.name = view.name
  newView.id = (view as any).id
  newView.links = (view as any).links

  return newView
}
