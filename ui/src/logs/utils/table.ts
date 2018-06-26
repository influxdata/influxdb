import _ from 'lodash'
import moment from 'moment'
import {getDeep} from 'src/utils/wrappers'
import {TableData, LogsTableColumn, SeverityFormat} from 'src/types/logs'
import {SeverityFormatOptions} from 'src/logs/constants'

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
  _.includes(['appname', 'facility', 'host', 'hostname', 'severity'], column)

export const formatColumnValue = (
  column: string,
  value: string,
  charLimit: number
): string => {
  switch (column) {
    case 'timestamp':
      return moment(+value / 1000000).format('YYYY/MM/DD HH:mm:ss')
    case 'time':
      return moment(+value / 1000000).format('YYYY/MM/DD HH:mm:ss')
    case 'message':
      value = (value || 'No Message Provided').replace('\\n', '')
      if (value.indexOf(' ') > charLimit - 5) {
        value = _.truncate(value, {length: charLimit - 5})
      }
      return value
  }
  return value
}
export const header = (
  key: string,
  headerOptions: LogsTableColumn[]
): string => {
  if (key === SeverityFormatOptions.dot) {
    return ''
  }

  const headerOption = _.find(headerOptions, h => h.internalName === key)
  return _.get(headerOption, 'displayName') || _.capitalize(key)
}

export const getColumnWidth = (column: string): number => {
  return _.get(
    {
      timestamp: 160,
      procid: 80,
      facility: 120,
      severity_dot: 25,
      severity_text: 120,
      severity_dotText: 120,
      host: 300,
    },
    column,
    200
  )
}

export const getMessageWidth = (
  data: TableData,
  tableColumns: LogsTableColumn[],
  severityFormat: SeverityFormat
): number => {
  const columns = getColumnsFromData(data)
  const otherWidth = columns.reduce((acc, col) => {
    const colConfig = tableColumns.find(c => c.internalName === col)
    const isColVisible = colConfig && colConfig.visible
    if (col === 'message' || !isColVisible) {
      return acc
    }

    let columnName = col
    if (col === 'severity') {
      columnName = `${col}_${severityFormat}`
    }

    return acc + getColumnWidth(columnName)
  }, 0)

  const calculatedWidth = Math.max(
    window.innerWidth - (otherWidth + 180),
    100 * CHAR_WIDTH
  )

  return calculatedWidth - CHAR_WIDTH
}
