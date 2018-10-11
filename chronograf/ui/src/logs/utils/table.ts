import _ from 'lodash'
import moment from 'moment'
import {getDeep} from 'src/utils/wrappers'
import {
  TableData,
  LogsTableColumn,
  SeverityFormat,
  SeverityFormatOptions,
} from 'src/types/logs'
import {DEFAULT_TIME_FORMAT} from 'src/logs/constants'
import {
  orderTableColumns,
  filterTableColumns,
} from 'src/dashboards/utils/tableGraph'

export const ROW_HEIGHT = 18
const CHAR_WIDTH = 9
const DEFAULT_COLUMN_WIDTH = 200

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
  _.includes(
    ['appname', 'facility', 'host', 'hostname', 'severity', 'procid'],
    column
  )

export const formatColumnValue = (
  column: string,
  value: string,
  charLimit: number
): string => {
  switch (column) {
    case 'timestamp':
      return moment(+value / 1000000).format(DEFAULT_TIME_FORMAT)
    case 'procid':
    case 'host':
    case 'appname':
      return truncateText(value, column)
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
  if (key === SeverityFormatOptions.Dot) {
    return ''
  }

  const headerOption = _.find(headerOptions, h => h.internalName === key)
  return _.get(headerOption, 'displayName') || _.capitalize(key)
}

const truncateText = (value: string, column: string): string => {
  const columnWidth = getColumnWidth(column)
  const length = Math.floor(columnWidth / CHAR_WIDTH) - 2

  return _.truncate(value || '', {length})
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
    DEFAULT_COLUMN_WIDTH
  )
}

export const calculateRowCharWidth = (currentMessageWidth: number): number =>
  Math.floor(currentMessageWidth / CHAR_WIDTH)

export const calculateMessageHeight = (
  index: number,
  data: TableData,
  rowCharLimit: number
): number => {
  const columns = getColumnsFromData(data)
  const columnIndex = columns.indexOf('message')
  const value = getValueFromData(data, index, columnIndex)

  if (_.isEmpty(value)) {
    return ROW_HEIGHT
  }

  const lines = Math.ceil(value.length / (rowCharLimit * 1.25))

  return Math.max(lines, 1) * ROW_HEIGHT + 2
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
    if (col === 'message' || col === 'time' || !isColVisible) {
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

export const applyChangesToTableData = (
  tableData: TableData,
  tableColumns: LogsTableColumn[]
): TableData => {
  const columns = _.get(tableData, 'columns', [])
  const values = _.get(tableData, 'values', [])
  const data = [columns, ...values]

  const filteredData = filterTableColumns(data, tableColumns)
  const orderedData = orderTableColumns(filteredData, tableColumns)
  const updatedColumns: string[] = _.get(orderedData, '0', [])
  const updatedValues = _.slice(orderedData, 1)

  return {
    columns: updatedColumns,
    values: updatedValues,
  }
}

export const isEmptyInfiniteData = (data: {
  forward: TableData
  backward: TableData
}) => {
  return isEmptyTableData(data.forward) && isEmptyTableData(data.backward)
}

const isEmptyTableData = (data: TableData): boolean => {
  return getDeep(data, 'values.length', 0) === 0
}
