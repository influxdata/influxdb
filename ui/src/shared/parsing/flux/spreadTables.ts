import {FluxTable} from 'src/types'

export interface SeriesDescription {
  // A key identifying a unique (column, table, result) triple for a particular
  // Flux response—i.e. a single time series
  key: string
  // The name of the column that this series is extracted from (typically this
  // is `_value`, but could be any column)
  valueColumnName: string
  // The index of the column that this series was extracted from
  valueColumnIndex: number
  // The names and values of columns in the group key, plus the result name.
  // This provides the data for a user-recognizable label of the time series
  metaColumns: {
    [columnName: string]: string
  }
}

interface SpreadTablesResult {
  seriesDescriptions: SeriesDescription[]
  table: {
    [time: string]: {[seriesKey: string]: number}
  }
}

// Given a collection of `FluxTable`s parsed from a single Flux response,
// `spreadTables` will place each unique series found within the response into
// a single table, indexed by time. This data munging operation is often
// referred to as as a “spread”, “cast”, “pivot”, or “unfold”.
export const spreadTables = (tables: FluxTable[]): SpreadTablesResult => {
  const result: SpreadTablesResult = {
    table: {},
    seriesDescriptions: [],
  }

  for (const table of tables) {
    const header = table.data[0]

    if (!header) {
      continue
    }

    const seriesDescriptions = getSeriesDescriptions(table)
    const timeIndex = getTimeIndex(header)

    for (let i = 1; i < table.data.length; i++) {
      const row = table.data[i]
      const time = row[timeIndex]

      for (const {key, valueColumnIndex} of seriesDescriptions) {
        if (!result.table[time]) {
          result.table[time] = {}
        }

        result.table[time][key] = Number(row[valueColumnIndex])
      }
    }

    result.seriesDescriptions.push(...seriesDescriptions)
  }

  return result
}

const EXCLUDED_SERIES_COLUMNS = new Set([
  '_time',
  'result',
  'table',
  '_start',
  '_stop',
  '',
])

const NUMERIC_DATATYPES = new Set(['double', 'long', 'int', 'float'])

const getSeriesDescriptions = (table: FluxTable): SeriesDescription[] => {
  const seriesDescriptions = []
  const header = table.data[0]

  for (let i = 0; i < header.length; i++) {
    const columnName = header[i]
    const dataType = table.dataTypes[columnName]

    if (EXCLUDED_SERIES_COLUMNS.has(columnName)) {
      continue
    }

    if (table.groupKey[columnName]) {
      continue
    }

    if (!NUMERIC_DATATYPES.has(dataType)) {
      continue
    }

    const key = Object.entries(table.groupKey).reduce(
      (acc, [k, v]) => acc + `[${k}=${v}]`,
      `${columnName}[result=${table.result}]`
    )

    seriesDescriptions.push({
      key,
      valueColumnName: columnName,
      valueColumnIndex: i,
      metaColumns: {
        ...table.groupKey,
        result: table.result,
      },
    })
  }

  return seriesDescriptions
}

const getTimeIndex = header => {
  let timeIndex = header.indexOf('_time')

  if (timeIndex >= 0) {
    return timeIndex
  }

  timeIndex = header.indexOf('_start')
  if (timeIndex >= 0) {
    return timeIndex
  }

  timeIndex = header.indexOf('_end')
  if (timeIndex >= 0) {
    return timeIndex
  }

  return -1
}
