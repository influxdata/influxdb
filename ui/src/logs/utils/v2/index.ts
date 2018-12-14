import moment from 'moment'

import {DEFAULT_TIME_FORMAT} from 'src/shared/constants'

import {getDeep} from 'src/utils/wrappers'

import {FluxTable} from 'src/types'

export interface TableData {
  columns: string[]
  values: string[][]
}

export const formatTime = (time: number): string => {
  return moment(time).format(DEFAULT_TIME_FORMAT)
}

export const fluxToTableData = (
  tables: FluxTable[],
  columnNames: string[]
): TableData => {
  const values: string[][] = []
  const columns: string[] = []
  const indicesToKeep = []

  const rows = getDeep<string[][]>(tables, '0.data', [])
  const columnNamesRow = getDeep<string[]>(tables, '0.data.0', [])

  if (tables.length === 0) {
    return {columns: columnNames, values: []}
  }

  for (let i = 0; i < columnNames.length; i++) {
    const columnIndex = columnNamesRow.indexOf(columnNames[i])
    if (columnIndex !== -1) {
      indicesToKeep.push(columnIndex)
      columns.push(columnNames[i])
    }
  }

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i]
    const valuesForThisRow = []

    for (let j = 0; j < indicesToKeep.length; j++) {
      const index = indicesToKeep[j]
      valuesForThisRow.push(row[index])
    }

    values.push(valuesForThisRow)
  }

  return {
    columns,
    values,
  }
}
