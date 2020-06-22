// Libraries
import memoizeOne from 'memoize-one'
import moment from 'moment'
import {get, flatMap} from 'lodash'
import {fromFlux, Table} from '@influxdata/giraffe'

// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'
import {
  defaultXColumn,
  defaultYColumn,
  getNumericColumns as getNumericColumnsUtil,
  getGroupableColumns as getGroupableColumnsUtil,
} from 'src/shared/utils/vis'
import {getAllVariables, asAssignment} from 'src/variables/selectors'
import {getWindowPeriod} from 'src/variables/utils/getWindowVars'
import {
  timeRangeToDuration,
  parseDuration,
  durationToMilliseconds,
} from 'src/shared/utils/duration'

// Types
import {
  QueryView,
  DashboardQuery,
  FluxTable,
  AppState,
  DashboardDraftQuery,
  TimeRange,
} from 'src/types'

export const getActiveTimeMachine = (state: AppState) => {
  if (!state.timeMachines) {
    return null
  }

  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const timeMachine = timeMachines[activeTimeMachineID]

  return timeMachine
}

export const getIsInCheckOverlay = (state: AppState): boolean => {
  return state.timeMachines.activeTimeMachineID === 'alerting'
}

export const getActiveQuery = (state: AppState): DashboardDraftQuery => {
  const tm = getActiveTimeMachine(state)
  if (!tm) {
    return {
      text: '',
      hidden: true,
    }
  }

  const {draftQueries, activeQueryIndex} = tm

  return draftQueries[activeQueryIndex]
}

/*
  Get the value of the `v.windowPeriod` variable for the currently active query, in milliseconds.
*/
// TODO kill this function
export const getActiveWindowPeriod = (state: AppState) => {
  const {text} = getActiveQuery(state)
  const variables = getAllVariables(state).map(v => asAssignment(v))
console.log(text)

  return getWindowPeriod(text, variables)
}

const getTablesMemoized = memoizeOne((files: string[]): FluxTable[] =>
  files ? flatMap(files, parseResponse) : []
)

export const getTables = (state: AppState): FluxTable[] =>
  getTablesMemoized(getActiveTimeMachine(state).queryResults.files)

const getVisTableMemoized = memoizeOne(fromFlux)

export const getVisTable = (
  state: AppState
): {table: Table; fluxGroupKeyUnion: string[]} => {
  const files = getActiveTimeMachine(state).queryResults.files || []
  const {table, fluxGroupKeyUnion} = getVisTableMemoized(files.join('\n\n'))

  return {table, fluxGroupKeyUnion}
}

const getNumericColumnsMemoized = memoizeOne(getNumericColumnsUtil)

export const getNumericColumns = (state: AppState): string[] => {
  const {table} = getVisTable(state)

  return getNumericColumnsMemoized(table)
}

const getGroupableColumnsMemoized = memoizeOne(getGroupableColumnsUtil)

export const getGroupableColumns = (state: AppState): string[] => {
  const {table} = getVisTable(state)

  return getGroupableColumnsMemoized(table)
}

export const getXColumnSelection = (state: AppState): string => {
  const {table} = getVisTable(state)
  const preferredXColumnKey = get(
    getActiveTimeMachine(state),
    'view.properties.xColumn'
  )

  return defaultXColumn(table, preferredXColumnKey)
}

export const getYColumnSelection = (state: AppState): string => {
  const {table} = getVisTable(state)
  const preferredYColumnKey = get(
    getActiveTimeMachine(state),
    'view.properties.yColumn'
  )

  return defaultYColumn(table, preferredYColumnKey)
}

const getGroupableColumnSelection = (
  validColumns: string[],
  preference: string[],
  fluxGroupKeyUnion: string[]
): string[] => {
  if (preference && preference.every(col => validColumns.includes(col))) {
    return preference
  }

  return fluxGroupKeyUnion
}

const getFillColumnsSelectionMemoized = memoizeOne(getGroupableColumnSelection)

const getSymbolColumnsSelectionMemoized = memoizeOne(
  getGroupableColumnSelection
)

export const getFillColumnsSelection = (state: AppState): string[] => {
  const validFillColumns = getGroupableColumns(state)

  const preference = get(
    getActiveTimeMachine(state),
    'view.properties.fillColumns'
  )

  const {fluxGroupKeyUnion} = getVisTable(state)

  return getFillColumnsSelectionMemoized(
    validFillColumns,
    preference,
    fluxGroupKeyUnion
  )
}

export const getSymbolColumnsSelection = (state: AppState): string[] => {
  const validSymbolColumns = getGroupableColumns(state)
  const preference = get(
    getActiveTimeMachine(state),
    'view.properties.symbolColumns'
  )
  const {fluxGroupKeyUnion} = getVisTable(state)

  return getSymbolColumnsSelectionMemoized(
    validSymbolColumns,
    preference,
    fluxGroupKeyUnion
  )
}

export const getStartTime = (timeRange: TimeRange) => {
  if (!timeRange) {
    return Infinity
  }
  switch (timeRange.type) {
    case 'custom':
      return moment(timeRange.lower).valueOf()
    case 'selectable-duration':
      return moment()
        .subtract(timeRange.seconds, 'seconds')
        .valueOf()
    case 'duration':
      const millisecondDuration = durationToMilliseconds(
        parseDuration(timeRangeToDuration(timeRange))
      )
      return moment()
        .subtract(millisecondDuration, 'milliseconds')
        .valueOf()
    default:
      throw new Error(
        'unknown timeRange type ${timeRange.type} provided to getStartTime'
      )
  }
}

export const getEndTime = (timeRange: TimeRange): number => {
  if (!timeRange) {
    return null
  }
  if (timeRange.type === 'custom') {
    return moment(timeRange.upper).valueOf()
  }
  return moment().valueOf()
}

export const getActiveTimeRange = (
  timeRange: TimeRange,
  queries: DashboardQuery[]
) => {
  if (!queries) {
    return timeRange
  }
  const hasVariableTimes = queries.some(
    query =>
      query.text.includes('v.timeRangeStart') ||
      query.text.includes('v.timeRangeStop')
  )
  if (hasVariableTimes) {
    return timeRange
  }
  return null
}

export const getSaveableView = (state: AppState): QueryView & {id?: string} => {
  const {view, draftQueries} = getActiveTimeMachine(state)

  let saveableView: QueryView & {id?: string} = {
    ...view,
    properties: {
      ...view.properties,
      queries: draftQueries,
    },
  }

  // TODO: remove all of these conditionals

  if (saveableView.properties.type === 'histogram') {
    saveableView = {
      ...saveableView,
      properties: {
        ...saveableView.properties,
        xColumn: getXColumnSelection(state),
        fillColumns: getFillColumnsSelection(state),
      },
    }
  }

  if (saveableView.properties.type === 'heatmap') {
    saveableView = {
      ...saveableView,
      properties: {
        ...saveableView.properties,
        xColumn: getXColumnSelection(state),
        yColumn: getYColumnSelection(state),
      },
    }
  }

  if (saveableView.properties.type === 'scatter') {
    saveableView = {
      ...saveableView,
      properties: {
        ...saveableView.properties,
        xColumn: getXColumnSelection(state),
        yColumn: getYColumnSelection(state),
        fillColumns: getFillColumnsSelection(state),
        symbolColumns: getSymbolColumnsSelection(state),
      },
    }
  }

  if (saveableView.properties.type === 'xy') {
    saveableView = {
      ...saveableView,
      properties: {
        ...saveableView.properties,
        xColumn: getXColumnSelection(state),
        yColumn: getYColumnSelection(state),
      },
    }
  }

  if (saveableView.properties.type === 'line-plus-single-stat') {
    saveableView = {
      ...saveableView,
      properties: {
        ...saveableView.properties,
        xColumn: getXColumnSelection(state),
        yColumn: getYColumnSelection(state),
      },
    }
  }

  return saveableView
}
