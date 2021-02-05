// Libraries
import {cloneDeep, isNumber, get, map} from 'lodash'
import {produce} from 'immer'

// Utils
import {createView, defaultViewQuery} from 'src/views/helpers'
import {isConfigValid, buildQuery} from 'src/timeMachine/utils/queryBuilder'

// Constants
import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'
import {
  THRESHOLD_TYPE_TEXT,
  THRESHOLD_TYPE_BG,
} from 'src/shared/constants/thresholds'
import {pastHourTimeRange} from 'src/shared/constants/timeRanges'
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'
import {
  AGG_WINDOW_AUTO,
  DEFAULT_FILLVALUES,
} from 'src/timeMachine/constants/queryBuilder'

// Types
import {
  AutoRefresh,
  StatusRow,
  TableViewProperties,
  TimeRange,
  View,
  ViewType,
  QueryView,
  QueryViewProperties,
  ExtractWorkingView,
  DashboardDraftQuery,
  BuilderConfig,
  BuilderConfigAggregateWindow,
  RemoteDataState,
  TimeMachineID,
  Color,
} from 'src/types'
import {Action} from 'src/timeMachine/actions'
import {TimeMachineTab} from 'src/types/timeMachine'
import {BuilderAggregateFunctionType} from 'src/client/generatedRoutes'

interface QueryBuilderState {
  buckets: string[]
  bucketsStatus: RemoteDataState
  functions: Array<[{name: string}]>
  aggregateWindow: BuilderConfigAggregateWindow
  tags: Array<{
    aggregateFunctionType: BuilderAggregateFunctionType
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
  statuses: StatusRow[][]
}

export interface TimeMachineState {
  view: QueryView
  timeRange: TimeRange
  autoRefresh: AutoRefresh
  draftQueries: DashboardDraftQuery[]
  isViewingRawData: boolean
  isViewingVisOptions: boolean
  activeTab: TimeMachineTab
  activeQueryIndex: number | null
  queryBuilder: QueryBuilderState
  queryResults: QueryResultsState
  contextID?: string | null
}

export interface TimeMachinesState {
  activeTimeMachineID: TimeMachineID
  timeMachines: {
    ['de']: TimeMachineState
    ['veo']: TimeMachineState
    ['alerting']: TimeMachineState
  }
}

export const initialStateHelper = (): TimeMachineState => {
  return {
    timeRange: pastHourTimeRange,
    autoRefresh: AUTOREFRESH_DEFAULT,
    view: createView(),
    draftQueries: [{...defaultViewQuery(), hidden: false}],
    isViewingRawData: false,
    isViewingVisOptions: false,
    activeTab: 'queries',
    activeQueryIndex: 0,
    queryResults: initialQueryResultsState(),
    queryBuilder: {
      buckets: [],
      bucketsStatus: RemoteDataState.NotStarted,
      aggregateWindow: {
        period: AGG_WINDOW_AUTO,
        fillValues: DEFAULT_FILLVALUES,
      },
      functions: [[{name: 'mean'}]],
      tags: [
        {
          aggregateFunctionType: 'filter',
          keys: [],
          keysSearchTerm: '',
          keysStatus: RemoteDataState.NotStarted,
          values: [],
          valuesSearchTerm: '',
          valuesStatus: RemoteDataState.NotStarted,
        },
      ],
    },
  }
}

export const initialState = (): TimeMachinesState => ({
  activeTimeMachineID: 'de',
  timeMachines: {
    veo: initialStateHelper(),
    de: initialStateHelper(),
    alerting: initialStateHelper(),
  },
})

const getTableProperties = (view, files) => {
  if (!files || !files[0]) {
    return {...view.properties, fieldOptions: []}
  }
  const csv = files[0]
  let pointer = 0,
    ni

  // cut off the first 3 lines
  for (ni = 0; ni < 3; ni++) {
    pointer = csv.indexOf('\r\n', pointer) + 2
  }

  const existing = (view.properties.fieldOptions || []).reduce((prev, curr) => {
    prev[curr.internalName] = curr
    return prev
  }, {})

  csv
    .slice(pointer, csv.indexOf('\r\n', pointer))
    .split(',')
    .filter(o => !existing.hasOwnProperty(o))
    .filter(o => !['result', '', 'table', 'time'].includes(o))
    .forEach(o => {
      existing[o] = {
        internalName: o,
        displayName: o,
        visible: true,
      }
    })

  const fieldOptions = Object.keys(existing).map(e => existing[e])
  const properties = {...view.properties, fieldOptions}

  return properties
}

export const timeMachinesReducer = (
  state: TimeMachinesState = initialState(),
  action: Action
): TimeMachinesState => {
  if (action.type === 'SET_ACTIVE_TIME_MACHINE') {
    const {activeTimeMachineID, initialState} = action.payload
    const activeTimeMachine = state.timeMachines[activeTimeMachineID]
    const view = initialState.view || activeTimeMachine.view

    const draftQueries = map(cloneDeep(view.properties.queries), q => ({
      ...q,
      hidden: false,
    }))

    const queryBuilder = initialQueryBuilderState(draftQueries[0].builderConfig)

    const queryResults = initialQueryResultsState()
    const timeRange =
      activeTimeMachineID === 'alerting'
        ? DEFAULT_TIME_RANGE
        : activeTimeMachine.timeRange

    return {
      ...state,
      activeTimeMachineID,
      timeMachines: {
        ...state.timeMachines,
        [activeTimeMachineID]: {
          ...activeTimeMachine,
          activeTab: 'queries',
          timeRange,
          ...initialState,
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

    case 'SET_AUTO_REFRESH': {
      return produce(state, draftState => {
        draftState.autoRefresh = action.payload.autoRefresh

        buildAllQueries(draftState)
      })
    }

    case 'SET_VIEW_TYPE': {
      const {type} = action.payload
      const view = convertView(state.view, state.queryResults.files, type)

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
        const {
          status,
          files,
          fetchDuration,
          errorMessage,
          statuses,
        } = action.payload

        draftState.queryResults.status = status
        draftState.queryResults.errorMessage = errorMessage

        if (files && files.length) {
          if (
            state.view &&
            state.view.properties &&
            state.view.properties.type === 'table'
          ) {
            const properties = getTableProperties(state.view, files)
            draftState.view = {...state.view, properties}
          }
          draftState.queryResults.files = files
          draftState.queryResults.isInitialFetch = false
        }
        if (statuses) {
          draftState.queryResults.statuses = statuses
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

    case 'TOGGLE_VIS_OPTIONS': {
      return {...state, isViewingVisOptions: !state.isViewingVisOptions}
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

    case 'SET_X_COLUMN': {
      const {xColumn} = action.payload
      return setViewProperties(state, {xColumn})
    }

    case 'SET_Y_COLUMN': {
      const {yColumn} = action.payload
      return setViewProperties(state, {yColumn})
    }

    case 'SET_Y_SERIES_COLUMNS': {
      const {ySeriesColumns} = action.payload
      return setViewProperties(state, {ySeriesColumns})
    }

    case 'SET_X_AXIS_LABEL': {
      const {xAxisLabel} = action.payload

      switch (state.view.properties.type) {
        case 'histogram':
        case 'heatmap':
        case 'scatter':
        case 'mosaic':
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
        case 'mosaic':
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

    case 'SET_LINE_POSITION': {
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
        case 'band':
          return setYAxis(state, {prefix})
        default:
          return state
      }
    }

    case 'SET_TICK_PREFIX': {
      const {tickPrefix} = action.payload

      switch (state.view.properties.type) {
        case 'gauge':
        case 'single-stat':
        case 'line-plus-single-stat':
          return setViewProperties(state, {tickPrefix})
        case 'check':
        case 'xy':
        case 'band':
          return setYAxis(state, {tickPrefix})
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
        case 'band':
          return setYAxis(state, {suffix})
        default:
          return state
      }
    }

    case 'SET_TICK_SUFFIX': {
      const {tickSuffix} = action.payload

      switch (state.view.properties.type) {
        case 'gauge':
        case 'single-stat':
        case 'line-plus-single-stat':
          return setViewProperties(state, {tickSuffix})
        case 'check':
        case 'xy':
        case 'band':
          return setYAxis(state, {tickSuffix})
        default:
          return state
      }
    }

    case 'SET_COLORS': {
      const {colors} = action.payload

      switch (state.view.properties.type) {
        case 'gauge':
        case 'table':
        case 'single-stat':
        case 'scatter':
        case 'check':
        case 'xy':
        case 'band':
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

    case 'SET_HOVER_DIMENSION': {
      const {hoverDimension} = action.payload

      return setViewProperties(state, {hoverDimension})
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

    case 'EDIT_ACTIVE_QUERY_WITH_BUILDER': {
      return produce(state, draftState => {
        const query = draftState.draftQueries[draftState.activeQueryIndex]

        query.editMode = 'builder'
        query.hidden = false

        buildAllQueries(draftState)
      })
    }

    case 'RESET_QUERY_AND_EDIT_WITH_BUILDER': {
      return produce(state, draftState => {
        draftState.draftQueries[draftState.activeQueryIndex] = {
          ...defaultViewQuery(),
          editMode: 'builder',
          hidden: false,
        }
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

        if (activeQueryIndex < draftState.draftQueries.length) {
          draftState.activeQueryIndex = activeQueryIndex
          resetBuilderState(draftState)
        }
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

    case 'SET_BUILDER_AGGREGATE_FUNCTION_TYPE': {
      return produce(state, draftState => {
        const {index, builderAggregateFunctionType} = action.payload
        const draftQuery = draftState.draftQueries[draftState.activeQueryIndex]

        buildActiveQuery(draftState)
        if (
          draftQuery &&
          draftQuery.builderConfig &&
          draftQuery.builderConfig.tags[index]
        ) {
          // When switching between filtering and grouping
          // we want to clear out any previously selected values
          draftQuery.builderConfig.tags[index].values = []

          draftQuery.builderConfig.tags[
            index
          ].aggregateFunctionType = builderAggregateFunctionType
        }
      })
    }

    case 'SET_BUILDER_BUCKET_SELECTION': {
      return produce(state, draftState => {
        const builderConfig =
          draftState.draftQueries[draftState.activeQueryIndex].builderConfig

        builderConfig.buckets = [action.payload.bucket]

        if (action.payload.resetSelections) {
          const defaultAggregateFunctionType = initialStateHelper().queryBuilder
            .tags[0].aggregateFunctionType

          builderConfig.tags = [
            {
              key: '',
              values: [],
              aggregateFunctionType: defaultAggregateFunctionType,
            },
          ]
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
          tags[index].valuesStatus = RemoteDataState.NotStarted
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
        tag.aggregateFunctionType = 'filter'
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
        const [initialTags] = initialStateHelper().queryBuilder.tags

        draftQuery.builderConfig.tags.push({
          key: '',
          values: [],
          aggregateFunctionType: initialTags.aggregateFunctionType,
        })
        draftState.queryBuilder.tags.push(initialTags)
      })
    }

    case 'REMOVE_TAG_SELECTOR': {
      return produce(state, draftState => {
        const {index} = action.payload
        const draftQuery = draftState.draftQueries[draftState.activeQueryIndex]

        let selectedValues = []
        if (draftQuery) {
          selectedValues = draftQuery.builderConfig.tags[index].values

          draftQuery.builderConfig.tags.splice(index, 1)
          draftState.queryBuilder.tags.splice(index, 1)
        }

        if (selectedValues.length) {
          buildActiveQuery(draftState)
        }
      })
    }

    case 'SELECT_BUILDER_FUNCTION': {
      return produce(state, draftState => {
        const {functions} = action.payload

        const functionsWithNames = functions.map(f => ({
          name: f,
        }))

        draftState.draftQueries[
          draftState.activeQueryIndex
        ].builderConfig.functions = functionsWithNames

        buildActiveQuery(draftState)
      })
    }

    case 'SET_AGGREGATE_WINDOW': {
      return produce(state, draftState => {
        const {activeQueryIndex, draftQueries} = draftState
        const {period} = action.payload

        draftQueries[
          activeQueryIndex
        ].builderConfig.aggregateWindow.period = period
        buildActiveQuery(draftState)
      })
    }

    case 'SET_AGGREGATE_FILL_VALUES': {
      return produce(state, draftState => {
        const {activeQueryIndex, draftQueries} = draftState
        const {fillValues} = action.payload

        draftQueries[
          activeQueryIndex
        ].builderConfig.aggregateWindow.fillValues = fillValues
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
      const workingView = state.view
      const {fieldOptions} = action.payload
      const properties = {
        ...workingView.properties,
        fieldOptions,
      }
      const view = {...state.view, properties}
      return {...state, view}
    }

    case 'UPDATE_FIELD_OPTION': {
      const workingView = state.view as ExtractWorkingView<TableViewProperties>
      const {option} = action.payload
      const field = option.internalName

      const properties = {...workingView.properties}
      properties.fieldOptions = properties.fieldOptions.slice(0)

      const names = workingView.properties.fieldOptions.map(o => o.internalName)
      const idx = names.indexOf(field)

      if (idx < 0) {
        return state
      }

      properties.fieldOptions[idx] = option

      const view = {...state.view, properties}
      return {...state, view}
    }

    case 'SET_TABLE_OPTIONS': {
      const workingView = state.view
      const {tableOptions} = action.payload
      const properties = {...workingView.properties, tableOptions}
      const view = {...state.view, properties}

      return {...state, view}
    }

    case 'SET_TIME_FORMAT': {
      const workingView = state.view

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

  if (get(update, '0.type', '') === 'scale') {
    return [...colors.filter(c => c.type !== 'scale'), ...update]
  }
  return [...colors.filter(c => c.type === 'scale'), ...update]
}

const convertView = (
  view: QueryView,
  files,
  outType: ViewType
): View<QueryViewProperties> => {
  const newView: any = createView(outType)
  newView.properties.queries = cloneDeep(view.properties.queries)
  if (outType === 'table' && files) {
    newView.properties = getTableProperties(newView, files)
  }
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
  const defaultFunctions = initialStateHelper().queryBuilder.functions
  const [defaultTag] = initialStateHelper().queryBuilder.tags
  return {
    buckets: builderConfig.buckets,
    bucketsStatus: RemoteDataState.NotStarted,
    functions: [...defaultFunctions],
    aggregateWindow: {period: AGG_WINDOW_AUTO, fillValues: DEFAULT_FILLVALUES},
    tags: builderConfig.tags.map(() => {
      return {...defaultTag}
    }),
  }
}

const initialQueryResultsState = (): QueryResultsState => ({
  files: null,
  status: RemoteDataState.NotStarted,
  isInitialFetch: true,
  fetchDuration: null,
  errorMessage: null,
  statuses: null,
})

export const buildActiveQuery = (draftState: TimeMachineState) => {
  const draftQuery = draftState.draftQueries[draftState.activeQueryIndex]

  if (isConfigValid(draftQuery.builderConfig)) {
    draftQuery.text = buildQuery(draftQuery.builderConfig)
  } else if (!draftQuery.text) {
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
