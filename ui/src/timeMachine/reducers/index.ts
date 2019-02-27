// Libraries
import {get, cloneDeep} from 'lodash'
import {produce} from 'immer'
import _ from 'lodash'

// Utils
import {createView, defaultViewQuery} from 'src/shared/utils/view'
import {isConfigValid, buildQuery} from 'src/timeMachine/utils/queryBuilder'

// Constants
import {
  VEO_TIME_MACHINE_ID,
  DE_TIME_MACHINE_ID,
} from 'src/timeMachine/constants'

// Types
import {TimeRange, View} from 'src/types/v2'
import {
  ViewType,
  DashboardDraftQuery,
  BuilderConfig,
  InfluxLanguage,
  QueryEditMode,
  QueryView,
  QueryViewProperties,
  ExtractWorkingView,
} from 'src/types/v2/dashboards'
import {Action} from 'src/timeMachine/actions'
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {RemoteDataState} from 'src/types'
import {Color} from 'src/types/colors'

interface QueryBuilderState {
  buckets: string[]
  bucketsStatus: RemoteDataState
  tags: Array<{
    valuesSearchTerm: string
    keysSearchTerm: string
    keys: string[]
    keysStatus: RemoteDataState
    values: string[]
    valuesStatus: RemoteDataState
  }>
}

export interface TimeMachineState {
  view: QueryView
  timeRange: TimeRange
  draftQueries: DashboardDraftQuery[]
  isViewingRawData: boolean
  activeTab: TimeMachineTab
  activeQueryIndex: number | null
  submitToken: number
  availableXColumns: string[]
  availableGroupColumns: string[]
  queryBuilder: QueryBuilderState
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
  draftQueries: [{...defaultViewQuery(), hidden: false, manuallyEdited: false}],
  isViewingRawData: false,
  activeTab: TimeMachineTab.Queries,
  activeQueryIndex: 0,
  submitToken: 0,
  availableXColumns: [],
  availableGroupColumns: [],
  queryBuilder: {
    buckets: [],
    bucketsStatus: RemoteDataState.NotStarted,
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
    const draftQueries = _.map(cloneDeep(view.properties.queries), q => ({
      ...q,
      hidden: false,
      manuallyEdited: false,
    }))
    const queryBuilder = initialQueryBuilderState(draftQueries[0].builderConfig)
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
          queryBuilder,
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

        buildAndSubmitAllQueries(draftState)
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

    case 'SET_ACTIVE_QUERY_EDITED': {
      const {manuallyEdited} = action.payload
      const draftQueries = [...state.draftQueries]

      draftQueries[state.activeQueryIndex] = {
        ...draftQueries[state.activeQueryIndex],
        manuallyEdited,
      }

      return {...state, draftQueries}
    }

    case 'SUBMIT_SCRIPT': {
      return produce(state, draftState => {
        submitQueries(draftState)
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

    case 'TABLE_LOADED': {
      return produce(state, draftState => {
        const {availableXColumns, availableGroupColumns} = action.payload

        draftState.availableXColumns = availableXColumns
        draftState.availableGroupColumns = availableGroupColumns

        if (draftState.view.properties.type !== ViewType.Histogram) {
          return
        }

        const {xColumn, fillColumns} = draftState.view.properties
        const xColumnStale = !xColumn || !availableXColumns.includes(xColumn)
        const fillColumnsStale =
          !fillColumns ||
          fillColumns.some(name => !availableGroupColumns.includes(name))

        if (xColumnStale && availableXColumns.includes('_value')) {
          draftState.view.properties.xColumn = '_value'
        } else if (xColumnStale) {
          draftState.view.properties.xColumn = availableXColumns[0]
        }

        if (fillColumnsStale) {
          draftState.view.properties.fillColumns = []
        }
      })
    }

    case 'SET_X_COLUMN': {
      const {xColumn} = action.payload

      return setViewProperties(state, {xColumn})
    }

    case 'SET_X_AXIS_LABEL': {
      const {xAxisLabel} = action.payload

      return setViewProperties(state, {xAxisLabel})
    }

    case 'SET_FILL_COLUMNS': {
      const {fillColumns} = action.payload

      return setViewProperties(state, {fillColumns})
    }

    case 'SET_HISTOGRAM_POSITION': {
      const {position} = action.payload

      return setViewProperties(state, {position})
    }

    case 'SET_BIN_COUNT': {
      const {binCount} = action.payload

      return setViewProperties(state, {binCount})
    }

    case 'SET_VIEW_X_DOMAIN': {
      const {xDomain} = action.payload

      return setViewProperties(state, {xDomain})
    }

    case 'SET_PREFIX': {
      const {prefix} = action.payload

      switch (state.view.properties.type) {
        case ViewType.Gauge:
        case ViewType.SingleStat:
        case ViewType.LinePlusSingleStat:
          return setViewProperties(state, {prefix})
        case ViewType.XY:
          return setYAxis(state, {prefix})
        default:
          return state
      }
    }

    case 'SET_SUFFIX': {
      const {suffix} = action.payload

      switch (state.view.properties.type) {
        case ViewType.Gauge:
        case ViewType.SingleStat:
        case ViewType.LinePlusSingleStat:
          return setViewProperties(state, {suffix})
        case ViewType.XY:
          return setYAxis(state, {suffix})
        default:
          return state
      }
    }

    case 'SET_COLORS': {
      const {colors} = action.payload

      switch (state.view.properties.type) {
        case ViewType.Gauge:
        case ViewType.SingleStat:
        case ViewType.XY:
        case ViewType.Histogram:
          return setViewProperties(state, {colors})
        case ViewType.LinePlusSingleStat:
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

    case 'SET_BACKGROUND_THRESHOLD_COLORING': {
      const colors = state.view.properties.colors.map(color => {
        if (color.type !== 'scale') {
          return {
            ...color,
            type: 'background',
          }
        }

        return color
      })

      return setViewProperties(state, {colors})
    }

    case 'SET_TEXT_THRESHOLD_COLORING': {
      const colors = state.view.properties.colors.map(color => {
        if (color.type !== 'scale') {
          return {
            ...color,
            type: 'text',
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

    case 'INCREMENT_SUBMIT_TOKEN': {
      return {
        ...state,
        submitToken: Date.now(),
      }
    }

    case 'EDIT_ACTIVE_QUERY_WITH_BUILDER': {
      return produce(state, draftState => {
        const query = draftState.draftQueries[draftState.activeQueryIndex]

        query.editMode = QueryEditMode.Builder
        query.hidden = false

        buildAndSubmitAllQueries(draftState)
      })
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
          {...defaultViewQuery(), hidden: false, manuallyEdited: false},
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
        submitQueries(draftState)
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

const buildActiveQuery = (draftState: TimeMachineState) => {
  const draftQuery = draftState.draftQueries[draftState.activeQueryIndex]

  if (isConfigValid(draftQuery.builderConfig)) {
    draftQuery.text = buildQuery(draftQuery.builderConfig)
  } else {
    draftQuery.text = ''
  }
}

const buildAndSubmitAllQueries = (draftState: TimeMachineState) => {
  draftState.draftQueries
    .filter(query => query.editMode === QueryEditMode.Builder)
    .forEach(query => {
      if (isConfigValid(query.builderConfig)) {
        query.text = buildQuery(query.builderConfig)
      } else {
        query.text = ''
      }
    })

  submitQueries(draftState)
}

const resetBuilderState = (draftState: TimeMachineState) => {
  const newBuilderConfig =
    draftState.draftQueries[draftState.activeQueryIndex].builderConfig

  draftState.queryBuilder = initialQueryBuilderState(newBuilderConfig)
}

const submitQueries = (draftState: TimeMachineState) => {
  draftState.submitToken = Date.now()
  draftState.view.properties.queries = _.filter(
    draftState.draftQueries,
    q => !q.hidden
  )
}
