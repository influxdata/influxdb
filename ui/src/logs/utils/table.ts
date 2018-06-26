import _ from 'lodash'
import moment from 'moment'
import {getDeep} from 'src/utils/wrappers'
import {TableData} from 'src/types/logs'

const CHAR_WIDTH = 9

export const getValuesFromData = (data: TableData): string[][] =>
  getDeep(data, 'values', [])

export const getValueFromData = (
  data: TableData,
  row: number,
  column: number
): string => getDeep(data, `values.${row}.${column}`, '')

export const getColumnsFromData = (data: TableData): string[] =>
  getDeep<string[]>(data, 'columns', [])

export const getColumnFromData = (data: TableData, index: number): string =>
  getDeep(data, `columns.${index}`, '')

export const isClickable = (column: string): boolean =>
  _.includes(['appname', 'facility', 'host', 'hostname', 'severity_1'], column)

export const formatColumnValue = (
  column: string,
  value: string,
  charLimit: number
): string => {
  switch (column) {
    case 'timestamp':
      return moment(+value / 1000000).format('YYYY/MM/DD HH:mm:ss')
    case 'message':
      if (value) {
        if (value.indexOf(' ') > charLimit - 5) {
          return _.truncate(value, {length: charLimit - 5}).replace('\\n', '')
        } else {
          return value.replace('\\n', '')
        }
      }
      return ''

    default:
      return value
  }
}
export const header = (key: string): string => {
  return getDeep<string>(
    {
      timestamp: 'Timestamp',
      procid: 'Proc ID',
      message: 'Message',
      appname: 'Application',
      severity: '',
      severity_1: 'Severity',
    },
    key,
    _.capitalize(key)
  )
}

export const getColumnWidth = (column: string): number => {
  return _.get(
    {
      timestamp: 160,
      procid: 80,
      facility: 120,
      severity: 22,
      severity_1: 120,
      host: 300,
    },
    column,
    200
  )
}

export const getMessageWidth = (data: TableData): number => {
  const columns = getColumnsFromData(data)
  const otherWidth = columns.reduce((acc, col) => {
    if (col === 'message' || col === 'time') {
      return acc
    }

    return acc + getColumnWidth(col)
  }, 0)

  const calculatedWidth = Math.max(
    window.innerWidth - (otherWidth + 180),
    100 * CHAR_WIDTH
  )

  return calculatedWidth - CHAR_WIDTH
}
