import {FluxTable} from 'src/types'

const COLUMN_BLACKLIST = new Set([
  '_time',
  'result',
  'table',
  '_start',
  '_stop',
  '',
])

const NUMERIC_DATATYPES = ['double', 'long', 'int', 'float']

interface TableByTime {
  [time: string]: {[columnName: string]: string}
}
interface ParseTablesByTimeResult {
  tablesByTime: TableByTime[]
  allColumnNames: string[]
  nonNumericColumns: string[]
}

export const parseTablesByTime = (
  tables: FluxTable[]
): ParseTablesByTimeResult => {
  const allColumnNames = []
  const nonNumericColumns = []

  const tablesByTime = tables.map(table => {
    const header = table.data[0] as string[]
    const columnNames: {[k: number]: string} = {}

    for (let i = 0; i < header.length; i++) {
      const columnName = header[i]
      const dataType = table.dataTypes[columnName]

      if (COLUMN_BLACKLIST.has(columnName)) {
        continue
      }

      if (table.groupKey[columnName]) {
        continue
      }

      if (!NUMERIC_DATATYPES.includes(dataType)) {
        nonNumericColumns.push(columnName)
        continue
      }

      const uniqueColumnName = Object.entries(table.groupKey).reduce(
        (acc, [k, v]) => acc + `[${k}=${v}]`,
        columnName
      )

      columnNames[i] = uniqueColumnName
      allColumnNames.push(uniqueColumnName)
    }

    const timeIndex = header.indexOf('_time')

    if (timeIndex < 0) {
      throw new Error('Could not find time index in FluxTable')
    }

    const result = {}
    for (let i = 1; i < table.data.length; i++) {
      const row = table.data[i]
      const time = row[timeIndex].toString()

      result[time] = Object.entries(columnNames).reduce(
        (acc, [valueIndex, columnName]) => ({
          ...acc,
          [columnName]: row[valueIndex],
        }),
        {}
      )
    }

    return result
  })

  return {nonNumericColumns, tablesByTime, allColumnNames}
}
