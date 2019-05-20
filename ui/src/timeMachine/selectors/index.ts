// Libraries
import memoizeOne from 'memoize-one'
import {get, flatMap} from 'lodash'
import {isNumeric, fluxToTable, Table} from '@influxdata/vis'

// Utils
import {parseResponse} from 'src/shared/parsing/flux/response'

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

const getVisTableMemoized = memoizeOne(fluxToTable)

export const getVisTable = (state: AppState): Table => {
  const files = getActiveTimeMachine(state).queryResults.files || []
  const {table} = getVisTableMemoized(files.join('\n\n'))

  return table
}

const getNumericColumnsMemoized = memoizeOne(
  (table: Table): string[] => {
    const numericColumns = Object.entries(table.columns)
      .filter(
        ([__, {name, type}]) =>
          isNumeric(type) && name !== 'result' && name !== 'table'
      )
      .map(([name]) => name)

    return numericColumns
  }
)

export const getNumericColumns = (state: AppState): string[] => {
  const table = getVisTable(state)

  return getNumericColumnsMemoized(table)
}

const getGroupableColumnsMemoized = memoizeOne(
  (table: Table): string[] => {
    const invalidGroupColumns = new Set(['_value', '_start', '_stop', '_time'])
    const groupableColumns = Object.keys(table.columns).filter(
      name => !invalidGroupColumns.has(name)
    )

    return groupableColumns
  }
)

export const getGroupableColumns = (state: AppState): string[] => {
  const table = getVisTable(state)

  return getGroupableColumnsMemoized(table)
}

const selectXYColumn = (validColumns: string[], preference: string): string => {
  if (preference && validColumns.includes(preference)) {
    return preference
  }

  if (validColumns.includes('_value')) {
    return '_value'
  }

  if (validColumns.length) {
    return validColumns[0]
  }

  return null
}

const getXColumnSelectionMemoized = memoizeOne(selectXYColumn)

const getYColumnSelectionMemoized = memoizeOne(selectXYColumn)

export const getXColumnSelection = (state: AppState): string => {
  const validXColumns = getNumericColumns(state)
  const preference = get(getActiveTimeMachine(state), 'view.properties.xColumn')

  return getXColumnSelectionMemoized(validXColumns, preference)
}

export const getYColumnSelection = (state: AppState): string => {
  const validYColumns = getNumericColumns(state)
  const preference = get(getActiveTimeMachine(state), 'view.properties.yColumn')

  return getYColumnSelectionMemoized(validYColumns, preference)
}

const getFillColumnsSelectionMemoized = memoizeOne(
  (validFillColumns: string[], preference: string[]): string[] => {
    if (preference && preference.every(col => validFillColumns.includes(col))) {
      return preference
    }

    return []
  }
)

export const getFillColumnsSelection = (state: AppState): string[] => {
  const validFillColumns = getGroupableColumns(state)
  const preference = get(
    getActiveTimeMachine(state),
    'view.properties.fillColumns'
  )

  return getFillColumnsSelectionMemoized(validFillColumns, preference)
}

const getSymbolColumnsSelectionMemoized = memoizeOne(
  (validSymbolColumns: string[], preference: string[]): string[] => {
    if (
      preference &&
      preference.every(col => validSymbolColumns.includes(col))
    ) {
      return preference
    }

    return []
  }
)

export const getSymbolColumnsSelection = (state: AppState): string[] => {
  const validSymbolColumns = getGroupableColumns(state)
  const preference = get(
    getActiveTimeMachine(state),
    'view.properties.symbolColumns'
  )

  return getSymbolColumnsSelectionMemoized(validSymbolColumns, preference)
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

  return saveableView
}
