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
  response: GetTimeSeriesResult
): TableData => {
  const {tables} = response

  const values: TimeSeriesValue[][] = []
  const rows = getDeep<TimeSeriesValue[][]>(tables, '0.data', [])
  const columnNamesRow = getDeep<string[]>(tables, '0.data.0', [])
  const columns: string[] = columnNamesRow.slice(3)

  for (let i = 1; i < rows.length; i++) {
    values.push(rows[i].slice(3))
  }

  return {
    columns,
    values,
  }
}
