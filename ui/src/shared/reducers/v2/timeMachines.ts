// Libraries
import {get, cloneDeep} from 'lodash'

// Utils
import {createView} from 'src/shared/utils/view'

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
import {TimeMachineTab, TimeMachineEditor} from 'src/types/v2/timeMachine'

export interface TimeMachineState {
  view: View<QueryViewProperties> | NewView<QueryViewProperties>
  timeRange: TimeRange
  draftScripts: string[]
  isViewingRawData: boolean
  activeTab: TimeMachineTab
  activeQueryEditor: TimeMachineEditor
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
  draftScripts: [],
  isViewingRawData: false,
  activeTab: TimeMachineTab.Queries,
  activeQueryEditor: TimeMachineEditor.QueryBuilder,
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
    const queries = view.properties.queries
    const draftScripts: string[] = queries.map(
      (query: DashboardQuery) => query.text
    )
    const activeQueryIndex = 0
    const activeQueryEditor = getActiveQueryEditor(queries, activeQueryIndex)

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
          activeQueryEditor,
          draftScripts,
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

      return {...state, timeRange}
    }

    case 'SET_VIEW_TYPE': {
      const {type} = action.payload
      const view = convertView(state.view, type)

      return {...state, view}
    }

    case 'SET_DRAFT_SCRIPT': {
      const {draftScript} = action.payload
      const draftScripts = [...state.draftScripts]

      draftScripts[state.activeQueryIndex] = draftScript

      return {...state, draftScripts}
    }

    case 'SUBMIT_SCRIPT': {
      const {view, draftScripts} = state

      return {
        ...state,
        submitToken: Date.now(),
        view: replaceQueries(view, draftScripts),
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

    case 'EDIT_ACTIVE_QUERY_WITH_BUILDER': {
      const {view, activeQueryIndex} = state

      return {
        ...state,
        activeQueryEditor: TimeMachineEditor.QueryBuilder,
        view: changeQueryType(view, activeQueryIndex, QueryEditMode.Builder),
      }
    }

    case 'EDIT_ACTIVE_QUERY_AS_FLUX': {
      const {view, activeQueryIndex} = state

      return {
        ...state,
        activeQueryEditor: TimeMachineEditor.FluxEditor,
        view: changeQueryType(
          view,
          activeQueryIndex,
          QueryEditMode.Advanced,
          InfluxLanguage.Flux
        ),
      }
    }

    case 'EDIT_ACTIVE_QUERY_AS_INFLUXQL': {
      const {view, activeQueryIndex} = state

      return {
        ...state,
        activeQueryEditor: TimeMachineEditor.InfluxQLEditor,
        view: changeQueryType(
          view,
          activeQueryIndex,
          QueryEditMode.Advanced,
          InfluxLanguage.InfluxQL
        ),
      }
    }

    case 'SET_ACTIVE_QUERY_INDEX': {
      const {activeQueryIndex} = action.payload
      const queries: DashboardQuery[] = get(
        state,
        'view.properties.queries',
        []
      )
      const activeQueryEditor = getActiveQueryEditor(queries, activeQueryIndex)

      return {...state, activeQueryIndex, activeQueryEditor}
    }

    case 'ADD_QUERY': {
      const view = addQuery(state.view)
      const activeQueryIndex: number = view.properties.queries.length - 1

      const activeQueryEditor = getActiveQueryEditor(
        view.properties.queries,
        activeQueryIndex
      )

      return {...state, view, activeQueryIndex, activeQueryEditor}
    }

    case 'REMOVE_QUERY': {
      const {queryIndex} = action.payload
      const view = removeQuery(state.view, queryIndex)
      const draftScripts = state.draftScripts.filter(
        (__, i) => i !== queryIndex
      )
      const queryLength = view.properties.queries.length

      let activeQueryIndex: number

      if (queryIndex < queryLength) {
        activeQueryIndex = queryIndex
      } else if (queryLength === queryIndex && queryLength > 0) {
        activeQueryIndex = queryLength - 1
      } else {
        activeQueryIndex = 0
      }

      const activeQueryEditor = getActiveQueryEditor(
        view.properties.queries,
        activeQueryIndex
      )

      return {...state, view, activeQueryIndex, activeQueryEditor, draftScripts}
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

const getActiveQueryEditor = (
  queries: DashboardQuery[],
  activeQueryIndex: number
): TimeMachineEditor => {
  const editMode: QueryEditMode = get(
    queries,
    `${activeQueryIndex}.editMode`,
    QueryEditMode.Builder
  )

  const queryType: InfluxLanguage = get(
    queries,
    `${activeQueryIndex}.type`,
    InfluxLanguage.Flux
  )

  if (
    editMode === QueryEditMode.Advanced &&
    queryType === InfluxLanguage.Flux
  ) {
    return TimeMachineEditor.FluxEditor
  }

  if (
    editMode === QueryEditMode.Advanced &&
    queryType === InfluxLanguage.InfluxQL
  ) {
    return TimeMachineEditor.InfluxQLEditor
  }

  return TimeMachineEditor.QueryBuilder
}

export const convertView = <T extends View | NewView>(
  view: T,
  outType: ViewType
): T => {
  const newView: any = createView(outType)

  const oldViewQueries = get(view, 'properties.queries')
  const newViewQueries = get(newView, 'properties.queries')

  if (oldViewQueries && newViewQueries) {
    newView.properties.queries = cloneDeep(oldViewQueries)
  }

  newView.name = view.name
  newView.id = (view as any).id
  newView.links = (view as any).links

  return newView
}

// Replaces the text of the each query in a view
export const replaceQueries = <T extends View | NewView>(
  view: T,
  newQueryTexts: string[]
): T => {
  const anyView: any = view
  const queries = anyView.properties.queries

  if (!queries) {
    return view
  }

  const newQueries = [...queries]

  for (let i = 0; i < newQueryTexts.length; i++) {
    if (queries[i]) {
      newQueries[i] = {...queries[i], text: newQueryTexts[i]}
    } else {
      const query: DashboardQuery = {
        text: newQueryTexts[i],
        type: InfluxLanguage.Flux,
        sourceID: '',
        editMode: QueryEditMode.Builder,
      }

      newQueries[i] = query
    }
  }

  return {
    ...anyView,
    properties: {
      ...anyView.properties,
      queries: newQueries,
    },
  }
}

const addQuery = <T extends View | NewView>(view: T): T => {
  const anyView: any = view
  const queries = anyView.properties.queries

  if (!queries) {
    return view
  }

  const query: DashboardQuery = {
    text: '',
    type: InfluxLanguage.Flux,
    sourceID: '',
    editMode: QueryEditMode.Builder,
  }

  return {
    ...anyView,
    properties: {
      ...anyView.properties,
      queries: [...queries, query],
    },
  }
}

export function removeQuery<T extends View | NewView>(
  view: T,
  queryIndex: number
): T {
  const anyView: any = view
  const queries = anyView.properties.queries

  if (!queries || !queries[queryIndex]) {
    return view
  }

  return {
    ...anyView,
    properties: {
      ...anyView.properties,
      queries: queries.filter((__, i) => i !== queryIndex),
    },
  }
}

export function changeQueryType<T extends View | NewView>(
  view: T,
  queryIndex: number,
  editMode?: QueryEditMode,
  type?: InfluxLanguage
): T {
  const anyView: any = view
  const queries = anyView.properties.queries

  if (!queries || !queries[queryIndex]) {
    return view
  }

  const query = queries[queryIndex]
  const resolvedType = type || query.type
  const resolvedEditMode = editMode || query.editMode
  const newQueries = [...queries]

  newQueries[queryIndex] = {
    ...query,
    editMode: resolvedEditMode,
    type: resolvedType,
  }

  return {
    ...anyView,
    properties: {
      ...anyView.properties,
      queries: newQueries,
    },
  }
}
