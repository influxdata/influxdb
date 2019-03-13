// Libraries
import {get} from 'lodash'

// Actions
import {loadBuckets} from 'src/timeMachine/actions/queryBuilder'
import {refreshVariableValues} from 'src/variables/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {filterUnusedVars} from 'src/shared/utils/filterUnusedVars'
import {getVariablesForOrg} from 'src/variables/selectors'
import {getActiveOrg} from 'src/organizations/selectors'

// Types
import {Dispatch} from 'redux-thunk'
import {TimeMachineState} from 'src/timeMachine/reducers'
import {Action as QueryBuilderAction} from 'src/timeMachine/actions/queryBuilder'
import {TimeRange, ViewType, GetState} from 'src/types/v2'
import {
  Axes,
  DecimalPlaces,
  XYViewGeom,
  FieldOption,
  TableOptions,
} from 'src/types/v2/dashboards'
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {Color} from 'src/types/colors'
import {Table, HistogramPosition, isNumeric} from 'src/minard'

export type Action =
  | QueryBuilderAction
  | SetActiveTimeMachineAction
  | SetActiveTabAction
  | SetNameAction
  | SetTimeRangeAction
  | SetTypeAction
  | SetActiveQueryText
  | SubmitQueriesAction
  | SetIsViewingRawDataAction
  | SetGeomAction
  | SetDecimalPlaces
  | SetBackgroundThresholdColoringAction
  | SetTextThresholdColoringAction
  | SetAxes
  | SetStaticLegend
  | SetColors
  | SetYAxisLabel
  | SetYAxisMinBound
  | SetYAxisMaxBound
  | SetYAxisPrefix
  | SetYAxisSuffix
  | SetYAxisBase
  | SetYAxisScale
  | SetPrefix
  | SetSuffix
  | IncrementSubmitToken
  | SetActiveQueryIndexAction
  | AddQueryAction
  | RemoveQueryAction
  | ToggleQueryAction
  | EditActiveQueryAsFluxAction
  | EditActiveQueryWithBuilderAction
  | UpdateActiveQueryNameAction
  | SetFieldOptionsAction
  | SetTableOptionsAction
  | SetTimeFormatAction
  | SetXColumnAction
  | SetFillColumnsAction
  | SetBinCountAction
  | SetHistogramPositionAction
  | TableLoadedAction
  | SetXDomainAction
  | SetXAxisLabelAction

interface SetActiveTimeMachineAction {
  type: 'SET_ACTIVE_TIME_MACHINE'
  payload: {
    activeTimeMachineID: string
    initialState: Partial<TimeMachineState>
  }
}

export const setActiveTimeMachine = (
  activeTimeMachineID: string,
  initialState: Partial<TimeMachineState> = {}
): SetActiveTimeMachineAction => ({
  type: 'SET_ACTIVE_TIME_MACHINE',
  payload: {activeTimeMachineID, initialState},
})

interface SetActiveTabAction {
  type: 'SET_ACTIVE_TAB'
  payload: {activeTab: TimeMachineTab}
}

export const setActiveTab = (
  activeTab: TimeMachineTab
): SetActiveTabAction => ({
  type: 'SET_ACTIVE_TAB',
  payload: {activeTab},
})

interface SetNameAction {
  type: 'SET_VIEW_NAME'
  payload: {name: string}
}

export const setName = (name: string): SetNameAction => ({
  type: 'SET_VIEW_NAME',
  payload: {name},
})

interface SetTimeRangeAction {
  type: 'SET_TIME_RANGE'
  payload: {timeRange: TimeRange}
}

export const setTimeRange = (timeRange: TimeRange): SetTimeRangeAction => ({
  type: 'SET_TIME_RANGE',
  payload: {timeRange},
})

interface SetTypeAction {
  type: 'SET_VIEW_TYPE'
  payload: {type: ViewType}
}

export const setType = (type: ViewType): SetTypeAction => ({
  type: 'SET_VIEW_TYPE',
  payload: {type},
})

interface SetActiveQueryText {
  type: 'SET_ACTIVE_QUERY_TEXT'
  payload: {text: string}
}

export const setActiveQueryText = (text: string): SetActiveQueryText => ({
  type: 'SET_ACTIVE_QUERY_TEXT',
  payload: {text},
})

interface SubmitQueriesAction {
  type: 'SUBMIT_QUERIES'
}

export const submitQueries = (): SubmitQueriesAction => ({
  type: 'SUBMIT_QUERIES',
})

interface SetIsViewingRawDataAction {
  type: 'SET_IS_VIEWING_RAW_DATA'
  payload: {isViewingRawData: boolean}
}

export const setIsViewingRawData = (
  isViewingRawData: boolean
): SetIsViewingRawDataAction => ({
  type: 'SET_IS_VIEWING_RAW_DATA',
  payload: {isViewingRawData},
})

interface SetGeomAction {
  type: 'SET_GEOM'
  payload: {geom: XYViewGeom}
}

export const setGeom = (geom: XYViewGeom): SetGeomAction => ({
  type: 'SET_GEOM',
  payload: {geom},
})

interface SetAxes {
  type: 'SET_AXES'
  payload: {axes: Axes}
}

export const setAxes = (axes: Axes): SetAxes => ({
  type: 'SET_AXES',
  payload: {axes},
})

interface SetYAxisLabel {
  type: 'SET_Y_AXIS_LABEL'
  payload: {label: string}
}

export const setYAxisLabel = (label: string): SetYAxisLabel => ({
  type: 'SET_Y_AXIS_LABEL',
  payload: {label},
})

interface SetYAxisMinBound {
  type: 'SET_Y_AXIS_MIN_BOUND'
  payload: {min: string}
}

export const setYAxisMinBound = (min: string): SetYAxisMinBound => ({
  type: 'SET_Y_AXIS_MIN_BOUND',
  payload: {min},
})

interface SetYAxisMaxBound {
  type: 'SET_Y_AXIS_MAX_BOUND'
  payload: {max: string}
}

export const setYAxisMaxBound = (max: string): SetYAxisMaxBound => ({
  type: 'SET_Y_AXIS_MAX_BOUND',
  payload: {max},
})

interface SetYAxisPrefix {
  type: 'SET_Y_AXIS_PREFIX'
  payload: {prefix: string}
}

export const setYAxisPrefix = (prefix: string): SetYAxisPrefix => ({
  type: 'SET_Y_AXIS_PREFIX',
  payload: {prefix},
})

interface SetYAxisSuffix {
  type: 'SET_Y_AXIS_SUFFIX'
  payload: {suffix: string}
}

export const setYAxisSuffix = (suffix: string): SetYAxisSuffix => ({
  type: 'SET_Y_AXIS_SUFFIX',
  payload: {suffix},
})

interface SetYAxisBase {
  type: 'SET_Y_AXIS_BASE'
  payload: {base: string}
}

export const setYAxisBase = (base: string): SetYAxisBase => ({
  type: 'SET_Y_AXIS_BASE',
  payload: {base},
})

interface SetYAxisScale {
  type: 'SET_Y_AXIS_SCALE'
  payload: {scale: string}
}

export const setYAxisScale = (scale: string): SetYAxisScale => ({
  type: 'SET_Y_AXIS_SCALE',
  payload: {scale},
})

interface SetPrefix {
  type: 'SET_PREFIX'
  payload: {prefix: string}
}

export const setPrefix = (prefix: string): SetPrefix => ({
  type: 'SET_PREFIX',
  payload: {prefix},
})

interface SetSuffix {
  type: 'SET_SUFFIX'
  payload: {suffix: string}
}

export const setSuffix = (suffix: string): SetSuffix => ({
  type: 'SET_SUFFIX',
  payload: {suffix},
})

interface SetStaticLegend {
  type: 'SET_STATIC_LEGEND'
  payload: {staticLegend: boolean}
}

export const setStaticLegend = (staticLegend: boolean): SetStaticLegend => ({
  type: 'SET_STATIC_LEGEND',
  payload: {staticLegend},
})

interface SetColors {
  type: 'SET_COLORS'
  payload: {colors: Color[]}
}

export const setColors = (colors: Color[]): SetColors => ({
  type: 'SET_COLORS',
  payload: {colors},
})

interface SetDecimalPlaces {
  type: 'SET_DECIMAL_PLACES'
  payload: {decimalPlaces: DecimalPlaces}
}

export const setDecimalPlaces = (
  decimalPlaces: DecimalPlaces
): SetDecimalPlaces => ({
  type: 'SET_DECIMAL_PLACES',
  payload: {decimalPlaces},
})

interface SetBackgroundThresholdColoringAction {
  type: 'SET_BACKGROUND_THRESHOLD_COLORING'
}

export const setBackgroundThresholdColoring = (): SetBackgroundThresholdColoringAction => ({
  type: 'SET_BACKGROUND_THRESHOLD_COLORING',
})

interface SetTextThresholdColoringAction {
  type: 'SET_TEXT_THRESHOLD_COLORING'
}

export const setTextThresholdColoring = (): SetTextThresholdColoringAction => ({
  type: 'SET_TEXT_THRESHOLD_COLORING',
})

interface IncrementSubmitToken {
  type: 'INCREMENT_SUBMIT_TOKEN'
}

export const incrementSubmitToken = (): IncrementSubmitToken => ({
  type: 'INCREMENT_SUBMIT_TOKEN',
})

interface EditActiveQueryWithBuilderAction {
  type: 'EDIT_ACTIVE_QUERY_WITH_BUILDER'
}

export const editActiveQueryWithBuilder = (): EditActiveQueryWithBuilderAction => ({
  type: 'EDIT_ACTIVE_QUERY_WITH_BUILDER',
})

interface EditActiveQueryAsFluxAction {
  type: 'EDIT_ACTIVE_QUERY_AS_FLUX'
}

export const editActiveQueryAsFlux = (): EditActiveQueryAsFluxAction => ({
  type: 'EDIT_ACTIVE_QUERY_AS_FLUX',
})

interface SetActiveQueryIndexAction {
  type: 'SET_ACTIVE_QUERY_INDEX'
  payload: {activeQueryIndex: number}
}

export const setActiveQueryIndexSync = (
  activeQueryIndex: number
): SetActiveQueryIndexAction => ({
  type: 'SET_ACTIVE_QUERY_INDEX',
  payload: {activeQueryIndex},
})

export const setActiveQueryIndex = (activeQueryIndex: number) => (
  dispatch: Dispatch<Action>
) => {
  dispatch(setActiveQueryIndexSync(activeQueryIndex))
  dispatch(loadBuckets())
}

interface AddQueryAction {
  type: 'ADD_QUERY'
}

export const addQuerySync = (): AddQueryAction => ({
  type: 'ADD_QUERY',
})

export const addQuery = () => (dispatch: Dispatch<Action>) => {
  dispatch(addQuerySync())
  dispatch(loadBuckets())
}

interface RemoveQueryAction {
  type: 'REMOVE_QUERY'
  payload: {queryIndex: number}
}

export const removeQuerySync = (queryIndex: number): RemoveQueryAction => ({
  type: 'REMOVE_QUERY',
  payload: {queryIndex},
})

interface ToggleQueryAction {
  type: 'TOGGLE_QUERY'
  payload: {queryIndex: number}
}

export const toggleQuerySync = (queryIndex: number): ToggleQueryAction => ({
  type: 'TOGGLE_QUERY',
  payload: {queryIndex},
})

export const removeQuery = (queryIndex: number) => (
  dispatch: Dispatch<Action>
) => {
  dispatch(removeQuerySync(queryIndex))
  dispatch(loadBuckets())
}

export const toggleQuery = (queryIndex: number) => (
  dispatch: Dispatch<Action>
) => {
  dispatch(toggleQuerySync(queryIndex))
  dispatch(submitQueriesWithVars())
}

interface UpdateActiveQueryNameAction {
  type: 'UPDATE_ACTIVE_QUERY_NAME'
  payload: {queryName: string}
}

export const updateActiveQueryName = (
  queryName: string
): UpdateActiveQueryNameAction => ({
  type: 'UPDATE_ACTIVE_QUERY_NAME',
  payload: {queryName},
})

interface SetFieldOptionsAction {
  type: 'SET_FIELD_OPTIONS'
  payload: {
    fieldOptions: FieldOption[]
  }
}

export const setFieldOptions = (
  fieldOptions: FieldOption[]
): SetFieldOptionsAction => ({
  type: 'SET_FIELD_OPTIONS',
  payload: {fieldOptions},
})

interface SetTableOptionsAction {
  type: 'SET_TABLE_OPTIONS'
  payload: {
    tableOptions: TableOptions
  }
}

export const setTableOptions = (
  tableOptions: TableOptions
): SetTableOptionsAction => ({
  type: 'SET_TABLE_OPTIONS',
  payload: {tableOptions},
})

interface SetTimeFormatAction {
  type: 'SET_TIME_FORMAT'
  payload: {
    timeFormat: string
  }
}

export const setTimeFormat = (timeFormat: string): SetTimeFormatAction => ({
  type: 'SET_TIME_FORMAT',
  payload: {timeFormat},
})

interface SetXColumnAction {
  type: 'SET_X_COLUMN'
  payload: {xColumn: string}
}

export const setXColumn = (xColumn: string): SetXColumnAction => ({
  type: 'SET_X_COLUMN',
  payload: {xColumn},
})

interface SetFillColumnsAction {
  type: 'SET_FILL_COLUMNS'
  payload: {fillColumns: string[]}
}

export const setFillColumns = (
  fillColumns: string[]
): SetFillColumnsAction => ({
  type: 'SET_FILL_COLUMNS',
  payload: {fillColumns},
})

interface SetBinCountAction {
  type: 'SET_BIN_COUNT'
  payload: {binCount: number}
}

export const setBinCount = (binCount: number): SetBinCountAction => ({
  type: 'SET_BIN_COUNT',
  payload: {binCount},
})

interface SetHistogramPositionAction {
  type: 'SET_HISTOGRAM_POSITION'
  payload: {position: HistogramPosition}
}

export const setHistogramPosition = (
  position: HistogramPosition
): SetHistogramPositionAction => ({
  type: 'SET_HISTOGRAM_POSITION',
  payload: {position},
})

interface TableLoadedAction {
  type: 'TABLE_LOADED'
  payload: {
    availableXColumns: string[]
    availableGroupColumns: string[]
  }
}

export const tableLoaded = (table: Table): TableLoadedAction => {
  const availableXColumns = Object.entries(table.columns)
    .filter(([__, {type}]) => isNumeric(type) && type !== 'time')
    .map(([name]) => name)

  const invalidGroupColumns = new Set(['_value', '_start', '_stop', '_time'])

  const availableGroupColumns = Object.keys(table.columns).filter(
    name => !invalidGroupColumns.has(name)
  )

  return {
    type: 'TABLE_LOADED',
    payload: {
      availableXColumns,
      availableGroupColumns,
    },
  }
}

interface SetXDomainAction {
  type: 'SET_VIEW_X_DOMAIN'
  payload: {xDomain: [number, number]}
}

export const setXDomain = (xDomain: [number, number]): SetXDomainAction => ({
  type: 'SET_VIEW_X_DOMAIN',
  payload: {xDomain},
})

interface SetXAxisLabelAction {
  type: 'SET_X_AXIS_LABEL'
  payload: {xAxisLabel: string}
}

export const setXAxisLabel = (xAxisLabel: string): SetXAxisLabelAction => ({
  type: 'SET_X_AXIS_LABEL',
  payload: {xAxisLabel},
})

export const refreshTimeMachineVariableValues = () => async (
  dispatch,
  getState: GetState
) => {
  const contextID = getState().timeMachines.activeTimeMachineID

  // Find variables currently used by queries in the TimeMachine
  const {view, draftQueries} = getActiveTimeMachine(getState())
  const draftView = {
    ...view,
    properties: {...view.properties, queries: draftQueries},
  }
  const orgID = getActiveOrg(getState()).id
  const variables = getVariablesForOrg(getState(), orgID)
  const variablesInUse = filterUnusedVars(variables, [draftView])

  // Find variables whose values have already been loaded by the TimeMachine
  // (regardless of whether these variables are currently being used)
  const existingVariableIDs = Object.keys(
    get(getState(), `variables.values.${contextID}.values`, {})
  )

  // Refresh values for all variables with existing values and in use variables
  const variablesToRefresh = variables.filter(
    v => variablesInUse.includes(v) || existingVariableIDs.includes(v.id)
  )

  await dispatch(refreshVariableValues(contextID, orgID, variablesToRefresh))
}

export const submitQueriesWithVars = () => async dispatch => {
  await dispatch(refreshTimeMachineVariableValues())

  dispatch(submitQueries())
}
