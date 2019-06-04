// Libraries
import memoizeOne from 'memoize-one'
import {get, flatMap} from 'lodash'
import {fromFlux, Table} from '@influxdata/vis'

// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'
import {chooseYColumn, chooseXColumn} from 'src/shared/utils/vis'

// Types
import {
  FluxTable,
  QueryView,
  AppState,
  DashboardDraftQuery,
  ViewType,
} from 'src/types'

export const getActiveTimeMachine = (state: AppState) => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const timeMachine = timeMachines[activeTimeMachineID]

  return timeMachine
}

export const getActiveQuery = (state: AppState): DashboardDraftQuery => {
  const {draftQueries, activeQueryIndex} = getActiveTimeMachine(state)

  return draftQueries[activeQueryIndex]
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

const getNumericColumnsMemoized = memoizeOne(
  (table: Table): string[] => {
    const columnKeys = table.columnKeys
    return columnKeys.reduce((numericColumns, key) => {
      const columnType = table.getColumnType(key)
      const columnName = table.getColumnName(key)
      if (
        (columnType === 'number' || columnType === 'time') &&
        columnName !== 'result' &&
        columnName !== 'table'
      ) {
        numericColumns.push(columnName)
      }
      return numericColumns
    }, [])
  }
)

export const getNumericColumns = (state: AppState): string[] => {
  const {table} = getVisTable(state)

  return getNumericColumnsMemoized(table)
}

const getGroupableColumnsMemoized = memoizeOne(
  (table: Table): string[] => {
    const invalidGroupColumns = new Set(['_value', '_time', 'table'])
    const groupableColumns = table.columnKeys.filter(
      name => !invalidGroupColumns.has(name)
    )

    return groupableColumns
  }
)

export const getGroupableColumns = (state: AppState): string[] => {
  const {table} = getVisTable(state)

  return getGroupableColumnsMemoized(table)
}

const selectXYColumn = (
  validColumns: string[],
  preference: string,
  defaultSelection: string
): string => {
  if (preference && validColumns.includes(preference)) {
    return preference
  }

  return defaultSelection
}

const getXColumnSelectionMemoized = memoizeOne(selectXYColumn)

const getYColumnSelectionMemoized = memoizeOne(selectXYColumn)

export const getXColumnSelection = (state: AppState): string => {
  const validXColumns = getNumericColumns(state)

  const preference = get(getActiveTimeMachine(state), 'view.properties.xColumn')

  const {table} = getVisTable(state)
  const defaultSelection = chooseXColumn(table)

  return getXColumnSelectionMemoized(
    validXColumns,
    preference,
    defaultSelection
  )
}

export const getYColumnSelection = (state: AppState): string => {
  const validYColumns = getNumericColumns(state)

  const preference = get(getActiveTimeMachine(state), 'view.properties.yColumn')

  const {table} = getVisTable(state)

  const defaultSelection = chooseYColumn(table)

  return getYColumnSelectionMemoized(
    validYColumns,
    preference,
    defaultSelection
  )
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

  if (saveableView.properties.type === ViewType.Histogram) {
    saveableView = {
      ...saveableView,
      properties: {
        ...saveableView.properties,
        xColumn: getXColumnSelection(state),
        fillColumns: getFillColumnsSelection(state),
      },
    }
  }

  if (saveableView.properties.type === ViewType.Heatmap) {
    saveableView = {
      ...saveableView,
      properties: {
        ...saveableView.properties,
        xColumn: getXColumnSelection(state),
        yColumn: getYColumnSelection(state),
      },
    }
  }

  if (saveableView.properties.type === ViewType.Scatter) {
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

  if (saveableView.properties.type === ViewType.XY) {
    saveableView = {
      ...saveableView,
      properties: {
        ...saveableView.properties,
        xColumn: getXColumnSelection(state),
        yColumn: getYColumnSelection(state),
      },
    }
  }

  if (saveableView.properties.type === ViewType.LinePlusSingleStat) {
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
