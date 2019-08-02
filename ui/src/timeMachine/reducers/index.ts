// Libraries
import {get, cloneDeep, isNumber} from 'lodash'
import {produce} from 'immer'
import _ from 'lodash'

// Utils
import {createView, defaultViewQuery} from 'src/shared/utils/view'
import {isConfigValid, buildQuery} from 'src/timeMachine/utils/queryBuilder'

// Constants
import {TimeMachineIDs} from 'src/timeMachine/constants'
import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'
import {
  THRESHOLD_TYPE_TEXT,
  THRESHOLD_TYPE_BG,
} from 'src/shared/constants/thresholds'

// Types
import {TimeRange, View, AutoRefresh} from 'src/types'
import {
  ViewType,
  DashboardDraftQuery,
  BuilderConfig,
  BuilderConfigAggregateWindow,
  QueryView,
  QueryViewProperties,
  ExtractWorkingView,
} from 'src/types/dashboards'
import {Action} from 'src/timeMachine/actions'
import {TimeMachineTab} from 'src/types/timeMachine'
import {RemoteDataState} from 'src/types'
import {Color} from 'src/types/colors'

interface QueryBuilderState {
  buckets: string[]
  bucketsStatus: RemoteDataState
  functions: Array<[{name: string}]>
  aggregateWindow: BuilderConfigAggregateWindow
  tags: Array<{
    valuesSearchTerm: string
    keysSearchTerm: string
    keys: string[]
    keysStatus: RemoteDataState
    values: string[]
    valuesStatus: RemoteDataState
  }>
}

interface QueryResultsState {
  files: string[] | null
  status: RemoteDataState
  isInitialFetch: boolean
  fetchDuration: number
  errorMessage: string
}

export interface TimeMachineState {
  view: QueryView
  timeRange: TimeRange
  autoRefresh: AutoRefresh
  draftQueries: DashboardDraftQuery[]
  isViewingRawData: boolean
  activeTab: TimeMachineTab
  activeQueryIndex: number | null
  queryBuilder: QueryBuilderState
  queryResults: QueryResultsState
}

export interface TimeMachinesState {
  activeTimeMachineID: TimeMachineIDs
  timeMachines: {
    ['de']: TimeMachineState
    ['veo']: TimeMachineState
    ['alerting']: TimeMachineState
  }
}

export const initialStateHelper = (): TimeMachineState => ({
  timeRange: {lower: 'now() - 1h'},
  autoRefresh: AUTOREFRESH_DEFAULT,
  view: createView(),
  draftQueries: [{...defaultViewQuery(), hidden: false}],
  isViewingRawData: false,
  activeTab: 'queries',
  activeQueryIndex: 0,
  queryResults: initialQueryResultsState(),
  queryBuilder: {
    buckets: [],
    bucketsStatus: RemoteDataState.NotStarted,
    aggregateWindow: {period: 'auto'},
    functions: [],
    tags: [
      {
        valuesSearchTerm: '',
        keysSearchTerm: '',
        keys: [],
        keysStatus: RemoteDataState.NotStarted,
        values: [],
        valuesStatus: RemoteDataState.NotStarted,
      },
    ],
  },
})

export const initialState = (): TimeMachinesState => ({
  activeTimeMachineID: 'de',
  timeMachines: {
    ['veo']: initialStateHelper(),
    ['de']: initialStateHelper(),
    ['alerting']: initialStateHelper(),
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
    const draftQueries = _.map(cloneDeep(view.properties.queries), q => ({
      ...q,
      hidden: false,
    }))
    const queryBuilder = initialQueryBuilderState(draftQueries[0].builderConfig)
    const queryResults = initialQueryResultsState()

    return {
      ...state,
      activeTimeMachineID,
      timeMachines: {
        ...state.timeMachines,
        [activeTimeMachineID]: {
          ...activeTimeMachine,
          ...initialState,
          activeTab: 'queries',
          isViewingRawData: false,
          activeQueryIndex: 0,
          draftQueries,
          queryBuilder,
          queryResults,
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

  const s = {
    ...state,
    timeMachines: {
      ...timeMachines,
      [activeTimeMachineID]: newActiveTimeMachine,
    },
  }

  return s
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
      return produce(state, draftState => {
        draftState.timeRange = action.payload.timeRange

        buildAllQueries(draftState)
      })
    }

    case 'SET_AUTO_REFRESH': {
      return produce(state, draftState => {
        draftState.autoRefresh = action.payload.autoRefresh

        buildAllQueries(draftState)
      })
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

    case 'SET_QUERY_RESULTS': {
      return produce(state, draftState => {
        const {status, files, fetchDuration, errorMessage} = action.payload

        draftState.queryResults.status = status
        draftState.queryResults.errorMessage = errorMessage

        if (files) {
          draftState.queryResults.files = files
          draftState.queryResults.isInitialFetch = false
        }

        if (isNumber(fetchDuration)) {
          draftState.queryResults.fetchDuration = fetchDuration
        }
      })
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

    case 'SET_Y_AXIS_BOUNDS': {
      const {bounds} = action.payload

      return setYAxis(state, {bounds})
    }

    case 'SET_AXIS_PREFIX': {
      const {prefix, axis} = action.payload
      const viewType = state.view.properties.type

      if (viewType === 'heatmap' || viewType == 'scatter') {
        if (axis === 'x') {
          return setViewProperties(state, {xPrefix: prefix})
        }
        return setViewProperties(state, {yPrefix: prefix})
      }
      return setYAxis(state, {prefix})
    }

    case 'SET_AXIS_SUFFIX': {
      const {suffix, axis} = action.payload
      const viewType = state.view.properties.type

      if (viewType === 'heatmap' || viewType === 'scatter') {
        if (axis === 'x') {
          return setViewProperties(state, {xSuffix: suffix})
        }
        return setViewProperties(state, {ySuffix: suffix})
      }
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

    case 'SET_X_COLUMN': {
      const {xColumn} = action.payload

      return setViewProperties(state, {xColumn})
    }

    case 'SET_Y_COLUMN': {
      const {yColumn} = action.payload

      return setViewProperties(state, {yColumn})
    }

    case 'SET_X_AXIS_LABEL': {
      const {xAxisLabel} = action.payload

      switch (state.view.properties.type) {
        case 'histogram':
        case 'heatmap':
        case 'scatter':
          return setViewProperties(state, {xAxisLabel})
        default:
          return setYAxis(state, {label: xAxisLabel})
      }
    }

    case 'SET_Y_AXIS_LABEL': {
      const {yAxisLabel} = action.payload

      switch (state.view.properties.type) {
        case 'histogram':
        case 'heatmap':
        case 'scatter':
          return setViewProperties(state, {yAxisLabel})
        default:
          return setYAxis(state, {label: yAxisLabel})
      }
    }

    case 'SET_FILL_COLUMNS': {
      const {fillColumns} = action.payload

      return setViewProperties(state, {fillColumns})
    }

    case 'SET_SYMBOL_COLUMNS': {
      const {symbolColumns} = action.payload

      return setViewProperties(state, {symbolColumns})
    }

    case 'SET_HISTOGRAM_POSITION': {
      const {position} = action.payload

      return setViewProperties(state, {position})
    }

    case 'SET_BIN_COUNT': {
      const {binCount} = action.payload

      return setViewProperties(state, {binCount})
    }

    case 'SET_BIN_SIZE': {
      const {binSize} = action.payload

      return setViewProperties(state, {binSize})
    }

    case 'SET_COLOR_HEXES': {
      const {colors} = action.payload

      return setViewProperties(state, {colors})
    }

    case 'SET_VIEW_X_DOMAIN': {
      const {xDomain} = action.payload

      return setViewProperties(state, {xDomain})
    }

    case 'SET_VIEW_Y_DOMAIN': {
      const {yDomain} = action.payload

      return setViewProperties(state, {yDomain})
    }

    case 'SET_PREFIX': {
      const {prefix} = action.payload

      switch (state.view.properties.type) {
        case 'gauge':
        case 'single-stat':
        case 'line-plus-single-stat':
          return setViewProperties(state, {prefix})
        case 'check':
        case 'xy':
          return setYAxis(state, {prefix})
        default:
          return state
      }
    }

    case 'SET_SUFFIX': {
      const {suffix} = action.payload

      switch (state.view.properties.type) {
        case 'gauge':
        case 'single-stat':
        case 'line-plus-single-stat':
          return setViewProperties(state, {suffix})
        case 'check':
        case 'xy':
          return setYAxis(state, {suffix})
        default:
          return state
      }
    }

    case 'SET_COLORS': {
      const {colors} = action.payload

      switch (state.view.properties.type) {
        case 'gauge':
        case 'single-stat':
        case 'scatter':
        case 'check':
        case 'xy':
        case 'histogram':
          return setViewProperties(state, {colors})
        case 'line-plus-single-stat':
          return setViewProperties(state, {
            colors: updateCorrectColors(state, colors),
          })
        default:
          return state
      }
    }

    case 'SET_DECIMAL_PLACES': {
      const {decimalPlaces} = action.payload

      return setViewProperties(state, {decimalPlaces})
    }

    case 'SET_SHADE_BELOW': {
      const {shadeBelow} = action.payload

      return setViewProperties(state, {shadeBelow})
    }

    case 'SET_BACKGROUND_THRESHOLD_COLORING': {
      const viewColors = state.view.properties.colors as Color[]

      const colors = viewColors.map(color => {
        if (color.type !== 'scale') {
          return {
            ...color,
            type: THRESHOLD_TYPE_BG,
          }
        }

        return color
      })

      return setViewProperties(state, {colors})
    }

    case 'SET_TEXT_THRESHOLD_COLORING': {
      const viewColors = state.view.properties.colors as Color[]

      const colors = viewColors.map(color => {
        if (color.type !== 'scale') {
          return {
            ...color,
            type: THRESHOLD_TYPE_TEXT,
          }
        }
        return color
      })

      return setViewProperties(state, {colors})
    }

    case 'SET_STATIC_LEGEND': {
      const {staticLegend} = action.payload

      return setViewProperties(state, {staticLegend})
    }

    case 'EDIT_ACTIVE_QUERY_WITH_BUILDER': {
      return produce(state, draftState => {
        const query = draftState.draftQueries[draftState.activeQueryIndex]

        query.editMode = 'builder'
        query.hidden = false

        buildAllQueries(draftState)
      })
    }

    case 'EDIT_ACTIVE_QUERY_AS_FLUX': {
      const {activeQueryIndex} = state
      const draftQueries = [...state.draftQueries]

      draftQueries[activeQueryIndex] = {
        ...draftQueries[activeQueryIndex],
        editMode: 'advanced',
      }

      return {
        ...state,
        draftQueries,
      }
    }

    case 'SET_ACTIVE_QUERY_INDEX': {
      return produce(state, draftState => {
        const {activeQueryIndex} = action.payload

        draftState.activeQueryIndex = activeQueryIndex

        resetBuilderState(draftState)
      })
    }

    case 'ADD_QUERY': {
      return produce(state, draftState => {
        draftState.draftQueries = [
          ...state.draftQueries,
          {...defaultViewQuery(), hidden: false},
        ]
        draftState.activeQueryIndex = draftState.draftQueries.length - 1

        resetBuilderState(draftState)
      })
    }

    case 'REMOVE_QUERY': {
      return produce(state, draftState => {
        const {queryIndex} = action.payload

        draftState.draftQueries.splice(queryIndex, 1)

        const queryLength = draftState.draftQueries.length

        let activeQueryIndex: number

        if (queryIndex < queryLength) {
          activeQueryIndex = queryIndex
        } else if (queryLength === queryIndex && queryLength > 0) {
          activeQueryIndex = queryLength - 1
        } else {
          activeQueryIndex = 0
        }

        draftState.activeQueryIndex = activeQueryIndex

        resetBuilderState(draftState)
      })
    }

    case 'TOGGLE_QUERY': {
      return produce(state, draftState => {
        const draftQuery = draftState.draftQueries[action.payload.queryIndex]

        draftQuery.hidden = !draftQuery.hidden
      })
    }

    case 'SET_BUILDER_BUCKET_SELECTION': {
      return produce(state, draftState => {
        const builderConfig =
          draftState.draftQueries[draftState.activeQueryIndex].builderConfig

        builderConfig.buckets = [action.payload.bucket]

        if (action.payload.resetSelections) {
          builderConfig.tags = [{key: '', values: []}]
          builderConfig.functions = []
          buildActiveQuery(draftState)
        }
      })
    }

    case 'SET_BUILDER_BUCKETS': {
      return produce(state, draftState => {
        draftState.queryBuilder.buckets = action.payload.buckets
        draftState.queryBuilder.bucketsStatus = RemoteDataState.Done
      })
    }

    case 'SET_BUILDER_BUCKETS_STATUS': {
      return produce(state, draftState => {
        draftState.queryBuilder.bucketsStatus = action.payload.bucketsStatus
      })
    }

    case 'SET_BUILDER_TAGS_STATUS': {
      return produce(state, draftState => {
        const {status} = action.payload
        const tags = draftState.queryBuilder.tags

        for (const tag of tags) {
          tag.keysStatus = status
          tag.valuesStatus = status
        }
      })
    }

    case 'SET_BUILDER_TAG_KEYS': {
      return produce(state, draftState => {
        const {index, keys} = action.payload

        draftState.queryBuilder.tags[index].keys = keys
        draftState.queryBuilder.tags[index].keysStatus = RemoteDataState.Done
      })
    }

    case 'SET_BUILDER_TAG_KEYS_STATUS': {
      return produce(state, draftState => {
        const {index, status} = action.payload
        const tags = draftState.queryBuilder.tags

        tags[index].keysStatus = status

        if (status === RemoteDataState.Loading) {
          for (let i = index + 1; i < tags.length; i++) {
            tags[i].keysStatus = RemoteDataState.NotStarted
          }
        }
      })
    }

    case 'SET_BUILDER_TAG_VALUES': {
      return produce(state, draftState => {
        const {index, values} = action.payload

        draftState.queryBuilder.tags[index].values = values
        draftState.queryBuilder.tags[index].valuesStatus = RemoteDataState.Done
      })
    }

    case 'SET_BUILDER_TAG_VALUES_STATUS': {
      return produce(state, draftState => {
        const {index, status} = action.payload

        draftState.queryBuilder.tags[index].valuesStatus = status
      })
    }

    case 'SET_BUILDER_TAG_KEY_SELECTION': {
      return produce(state, draftState => {
        const {index, key} = action.payload
        const draftQuery = draftState.draftQueries[draftState.activeQueryIndex]
        const tag = draftQuery.builderConfig.tags[index]

        tag.key = key
        tag.values = []
      })
    }

    case 'SET_BUILDER_TAG_VALUES_SELECTION': {
      return produce(state, draftState => {
        const {index, values} = action.payload
        const draftQuery = draftState.draftQueries[draftState.activeQueryIndex]

        draftQuery.builderConfig.tags[index].values = values

        buildActiveQuery(draftState)
      })
    }

    case 'SET_BUILDER_VALUES_SEARCH_TERM': {
      return produce(state, draftState => {
        const {index, searchTerm} = action.payload

        draftState.queryBuilder.tags[index].valuesSearchTerm = searchTerm
      })
    }

    case 'SET_BUILDER_KEYS_SEARCH_TERM': {
      return produce(state, draftState => {
        const {index, searchTerm} = action.payload

        draftState.queryBuilder.tags[index].keysSearchTerm = searchTerm
      })
    }

    case 'ADD_TAG_SELECTOR': {
      return produce(state, draftState => {
        const draftQuery = draftState.draftQueries[draftState.activeQueryIndex]

        draftQuery.builderConfig.tags.push({key: '', values: []})
        draftState.queryBuilder.tags.push({
          valuesSearchTerm: '',
          keysSearchTerm: '',
          keys: [],
          keysStatus: RemoteDataState.NotStarted,
          values: [],
          valuesStatus: RemoteDataState.NotStarted,
        })
      })
    }

    case 'REMOVE_TAG_SELECTOR': {
      return produce(state, draftState => {
        const {index} = action.payload
        const draftQuery = draftState.draftQueries[draftState.activeQueryIndex]

        const selectedValues = draftQuery.builderConfig.tags[index].values

        draftQuery.builderConfig.tags.splice(index, 1)
        draftState.queryBuilder.tags.splice(index, 1)

        if (selectedValues.length) {
          buildActiveQuery(draftState)
        }
      })
    }

    case 'SELECT_BUILDER_FUNCTION': {
      return produce(state, draftState => {
        const {name} = action.payload
        const functions =
          draftState.draftQueries[draftState.activeQueryIndex].builderConfig
            .functions

        let newFunctions

        if (functions.find(f => f.name === name)) {
          newFunctions = functions.filter(f => f.name !== name)
        } else {
          newFunctions = [...functions, {name}]
        }

        draftState.draftQueries[
          draftState.activeQueryIndex
        ].builderConfig.functions = newFunctions

        buildActiveQuery(draftState)
      })
    }

    case 'SELECT_AGGREGATE_WINDOW': {
      return produce(state, draftState => {
        const {activeQueryIndex, draftQueries} = draftState
        const {period} = action.payload

        draftQueries[activeQueryIndex].builderConfig.aggregateWindow = {period}
        buildActiveQuery(draftState)
      })
    }

    case 'UPDATE_ACTIVE_QUERY_NAME': {
      const {activeQueryIndex} = state
      const {queryName} = action.payload
      const draftQueries = [...state.draftQueries]

      draftQueries[activeQueryIndex] = {
        ...draftQueries[activeQueryIndex],
        name: queryName,
      }

      return {...state, draftQueries}
    }

    case 'SET_FIELD_OPTIONS': {
      const workingView = state.view as ExtractWorkingView<
        typeof action.payload
      >
      const {fieldOptions} = action.payload
      const properties = {...workingView.properties, fieldOptions}
      const view = {...state.view, properties}

      return {...state, view}
    }

    case 'SET_TABLE_OPTIONS': {
      const workingView = state.view as ExtractWorkingView<
        typeof action.payload
      >
      const {tableOptions} = action.payload
      const properties = {...workingView.properties, tableOptions}
      const view = {...state.view, properties}

      return {...state, view}
    }

    case 'SET_TIME_FORMAT': {
      const workingView = state.view as ExtractWorkingView<
        typeof action.payload
      >

      const {timeFormat} = action.payload
      const properties = {...workingView.properties, timeFormat}
      const view = {...state.view, properties}

      return {...state, view}
    }

    case 'SAVE_DRAFT_QUERIES': {
      return produce(state, draftState => {
        draftState.view.properties.queries = draftState.draftQueries.filter(
          q => !q.hidden
        )
      })
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

  return {
    ...state,
    view: {...view, properties: {...properties, ...update}},
  }
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
      properties: {
        ...properties,
        axes: {...axes, y: {...yAxis, ...update}},
      },
    },
  }
}

const updateCorrectColors = (
  state: TimeMachineState,
  update: Color[]
): Color[] => {
  const view: any = state.view
  const colors = view.properties.colors

  if (_.get(update, '0.type', '') === 'scale') {
    return [...colors.filter(c => c.type !== 'scale'), ...update]
  }
  return [...colors.filter(c => c.type === 'scale'), ...update]
}

const convertView = (
  view: QueryView,
  outType: ViewType
): View<QueryViewProperties> => {
  const newView: any = createView(outType)

  newView.properties.queries = cloneDeep(view.properties.queries)
  newView.name = view.name
  newView.cellID = view.cellID
  newView.dashboardID = view.dashboardID
  newView.id = (view as any).id
  newView.links = (view as any).links

  return newView
}

const initialQueryBuilderState = (
  builderConfig: BuilderConfig
): QueryBuilderState => {
  return {
    buckets: builderConfig.buckets,
    bucketsStatus: RemoteDataState.NotStarted,
    functions: [],
    aggregateWindow: {period: 'auto'},
    tags: builderConfig.tags.map(_ => ({
      valuesSearchTerm: '',
      keysSearchTerm: '',
      keys: [],
      keysStatus: RemoteDataState.NotStarted,
      values: [],
      valuesStatus: RemoteDataState.NotStarted,
    })),
  }
}

const initialQueryResultsState = (): QueryResultsState => ({
  files: null,
  status: RemoteDataState.NotStarted,
  isInitialFetch: true,
  fetchDuration: null,
  errorMessage: null,
})

const buildActiveQuery = (draftState: TimeMachineState) => {
  const draftQuery = draftState.draftQueries[draftState.activeQueryIndex]

  if (isConfigValid(draftQuery.builderConfig)) {
    draftQuery.text = buildQuery(draftQuery.builderConfig)
  } else {
    draftQuery.text = ''
  }
}

const buildAllQueries = (draftState: TimeMachineState) => {
  draftState.draftQueries
    .filter(query => query.editMode === 'builder')
    .forEach(query => {
      if (isConfigValid(query.builderConfig)) {
        query.text = buildQuery(query.builderConfig)
      } else {
        query.text = ''
      }
    })
}

const resetBuilderState = (draftState: TimeMachineState) => {
  const newBuilderConfig =
    draftState.draftQueries[draftState.activeQueryIndex].builderConfig

  draftState.queryBuilder = initialQueryBuilderState(newBuilderConfig)
}
