import {range, flatMap, isFinite, isString} from 'lodash'
import {Table, NumericColumnData} from '@influxdata/giraffe'

/*
  Return a list of the maximum elements in `xs`, where the magnitude of each
  element is computed using the passed function `d`.
*/
const maxesBy = <X>(xs: X[], d: (x: X) => number): X[] => {
  let maxes = []
  let maxDist = -Infinity

  for (const x of xs) {
    const dist = d(x)

    if (dist > maxDist) {
      maxes = [x]
      maxDist = dist
    } else if (dist === maxDist && dist !== -Infinity) {
      maxes.push(x)
    }
  }

  return maxes
}

const EXCLUDED_COLUMNS = new Set([
  '_start',
  '_stop',
  '_time',
  'table',
  'result',
  '',
])

/*
  Determine if the values in a column should be considered in `latestValues`.
*/
const isValueCol = (table: Table, colKey: string): boolean => {
  const columnType = table.getColumnType(colKey)
  const columnName = table.getColumnName(colKey)

  return (
    (columnType === 'number' ||
      columnType === 'time' ||
      columnType === 'string') &&
    !EXCLUDED_COLUMNS.has(columnName)
  )
}

/*
  We sort the column keys that we pluck latest values from, so that:

  - Columns named `_value` have precedence
  - The returned latest values are in a somewhat stable order
*/
const sortTableKeys = (keyA: string, keyB: string): number => {
  if (keyA.includes('_value')) {
    return -1
  } else if (keyB.includes('_value')) {
    return 1
  } else {
    return keyA.localeCompare(keyB)
  }
}

/*
  Return a list of the most recent numeric values present in a `Table`.

  This utility searches any numeric column to find values, and uses the `_time`
  column as their associated timestamp.

  If the table only has one row, then a time column is not needed.
*/
export const latestValues = (table: Table): number[] => {
  const valueColsData = table.columnKeys
    .sort((a, b) => sortTableKeys(a, b))
    .filter(k => isValueCol(table, k))
    .map(k => table.getColumn(k)) as number[][]

  if (!valueColsData.length) {
    return []
  }

  const columnKeys = table.columnKeys

  // Fallback to `_stop` column if `_time` column missing otherwise return empty array.
  let timeColData: NumericColumnData = []

  if (columnKeys.includes('_time')) {
    timeColData = table.getColumn('_time', 'number')
  } else if (columnKeys.includes('_stop')) {
    timeColData = table.getColumn('_stop', 'number')
  }

  if (!timeColData && table.length !== 1) {
    return []
  }

  const d = (i: number) => {
    const time = timeColData[i]

    if (
      time &&
      valueColsData.some(colData => {
        return isFinite(colData[i]) || isString(colData[i])
      })
    ) {
      return time
    }

    return -Infinity
  }

  const latestRowIndices =
    table.length === 1 ? [0] : maxesBy(range(table.length), d)

  const latestValues = flatMap(latestRowIndices, i =>
    valueColsData.map(colData => colData[i])
  )

  const definedLatestValues = latestValues.filter(
    x => isFinite(x) || isString(x)
  )

  return definedLatestValues
}
