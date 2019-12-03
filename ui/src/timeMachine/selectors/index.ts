// Libraries
import memoizeOne from 'memoize-one'
import moment from 'moment'
import {get, flatMap} from 'lodash'
import {fromFlux, Table} from '@influxdata/giraffe'

// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'
import {getCheckVisTimeRange} from 'src/alerting/utils/vis'
import {
  defaultXColumn,
  defaultYColumn,
  getNumericColumns as getNumericColumnsUtil,
  getGroupableColumns as getGroupableColumnsUtil,
} from 'src/shared/utils/vis'
import {getVariableAssignments as getVariableAssignmentsForContext} from 'src/variables/selectors'
import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {getWindowPeriod} from 'src/variables/utils/getWindowVars'

// Types
import {
  FluxTable,
  QueryView,
  AppState,
  DashboardDraftQuery,
  TimeRange,
  VariableAssignment,
} from 'src/types'

export const getActiveTimeMachine = (state: AppState) => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const timeMachine = timeMachines[activeTimeMachineID]

  return timeMachine
}

export const getIsInCheckOverlay = (state: AppState): boolean => {
  return state.timeMachines.activeTimeMachineID === 'alerting'
}

export const getTimeRange = (state: AppState): TimeRange => {
  const {alerting, timeRange} = getActiveTimeMachine(state)

  if (!getIsInCheckOverlay(state)) {
    return timeRange
  }

  return getCheckVisTimeRange(alerting.check.every)
}

export const getActiveQuery = (state: AppState): DashboardDraftQuery => {
  const {draftQueries, activeQueryIndex} = getActiveTimeMachine(state)

  return draftQueries[activeQueryIndex]
}

export const getVariableAssignments = (
  state: AppState
): VariableAssignment[] => {
  return [
    ...getTimeRangeVars(getTimeRange(state)),
    ...getVariableAssignmentsForContext(
      state,
      state.timeMachines.activeTimeMachineID
    ),
  ]
}

/*
  Get the value of the `v.windowPeriod` variable for the currently active query, in milliseconds.
*/
export const getActiveWindowPeriod = (state: AppState) => {
  const {text} = getActiveQuery(state)
  const variables = getVariableAssignments(state)

  return getWindowPeriod(text, variables)
}

const getTablesMemoized = memoizeOne(
  (files: string[]): FluxTable[] => (files ? flatMap(files, parseResponse) : [])
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

export const getSaveableView = (state: AppState): QueryView & {id?: string} => {
  const {view, draftQueries} = getActiveTimeMachine(state)

  let saveableView: QueryView & {id?: string} = {
    ...view,
    properties: {
      ...view.properties,
      queries: draftQueries,
    },
  }

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

export const getStartTime = (timeRange: TimeRange): number => {
  if (!timeRange) {
    return Infinity
  }
  const {lower} = timeRange
  switch (lower) {
    case 'now() - 30d':
      return moment()
        .subtract(30, 'days')
        .valueOf()
    case 'now() - 7d':
      return moment()
        .subtract(7, 'days')
        .valueOf()
    case 'now() - 2d':
      return moment()
        .subtract(2, 'days')
        .valueOf()
    case 'now() - 24h':
      return moment()
        .subtract(24, 'hours')
        .valueOf()
    case 'now() - 12h':
      return moment()
        .subtract(12, 'hours')
        .valueOf()
    case 'now() - 6h':
      return moment()
        .subtract(6, 'hours')
        .valueOf()
    case 'now() - 1h':
      return moment()
        .subtract(1, 'hours')
        .valueOf()
    case 'now() - 15m':
      return moment()
        .subtract(15, 'minutes')
        .valueOf()
    case 'now() - 5m':
      return moment()
        .subtract(5, 'minutes')
        .valueOf()
    default:
      return moment(lower).valueOf()
  }
}

export const getEndTime = (timeRange: TimeRange): number => {
  if (!timeRange) {
    return null
  }
  const {lower, upper} = timeRange
  if (lower.includes('now()')) {
    return moment().valueOf()
  }
  if (upper) {
    return moment(upper).valueOf()
  }
  return null
}
