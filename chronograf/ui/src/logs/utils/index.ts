import moment from 'moment'
import _ from 'lodash'

import {GetTimeSeriesResult} from 'src/flux/apis/index'
import {DEFAULT_TIME_FORMAT} from 'src/logs/constants'
import {TimeSeriesValue} from 'src/types/series'

import {getDeep} from 'src/utils/wrappers'

export interface TableData {
  columns: string[]
  values: TimeSeriesValue[][]
}

export const formatTime = (time: number): string => {
  return moment(time).format(DEFAULT_TIME_FORMAT)
}

export const transformFluxLogsResponse = (
  response: GetTimeSeriesResult,
  columnNames: string[]
): TableData => {
  const {tables} = response

  const values: TimeSeriesValue[][] = []
  const columns: string[] = []
  const indicesToKeep = []

  const rows = getDeep<TimeSeriesValue[][]>(tables, '0.data', [])
  const columnNamesRow = getDeep<string[]>(tables, '0.data.0', [])

  for (let i = 0; i < columnNamesRow.length; i++) {
    if (columnNames.includes(columnNamesRow[i])) {
      indicesToKeep.push(i)
      columns.push(columnNamesRow[i])
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
