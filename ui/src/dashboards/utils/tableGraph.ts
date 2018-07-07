import calculateSize from 'calculate-size'
import _ from 'lodash'
import {fastMap, fastReduce, fastFilter} from 'src/utils/fast'

import {CELL_HORIZONTAL_PADDING} from 'src/shared/constants/tableGraph'
import {
  DEFAULT_TIME_FIELD,
  DEFAULT_TIME_FORMAT,
  TimeField,
} from 'src/dashboards/constants'
import {
  Sort,
  FieldOption,
  TableOptions,
  DecimalPlaces,
} from 'src/types/dashboards'
import {TimeSeriesValue} from 'src/types/series'

interface ColumnWidths {
  totalWidths: number
  widths: {[x: string]: number}
}

interface SortedLabel {
  label: string
  responseIndex: number
  seriesIndex: number
}

interface TransformTableDataReturnType {
  transformedData: TimeSeriesValue[][]
  sortedTimeVals: TimeSeriesValue[]
  columnWidths: ColumnWidths
}

const calculateTimeColumnWidth = (timeFormat: string): number => {
  // Force usage of longest format names for ideal measurement
  timeFormat = _.replace(timeFormat, 'MMMM', 'September')
  timeFormat = _.replace(timeFormat, 'dddd', 'Wednesday')
  timeFormat = _.replace(timeFormat, 'A', 'AM')
  timeFormat = _.replace(timeFormat, 'h', '00')
  timeFormat = _.replace(timeFormat, 'X', '1522286058')

  const {width} = calculateSize(timeFormat, {
    font: '"RobotoMono", monospace',
    fontSize: '13px',
    fontWeight: 'bold',
  })

  return width + CELL_HORIZONTAL_PADDING
}

const updateMaxWidths = (
  row: TimeSeriesValue[],
  maxColumnWidths: ColumnWidths,
  topRow: TimeSeriesValue[],
  isTopRow: boolean,
  fieldOptions: FieldOption[],
  timeFormatWidth: number,
  verticalTimeAxis: boolean,
  decimalPlaces: DecimalPlaces
): ColumnWidths => {
  const maxWidths = fastReduce<TimeSeriesValue>(
    row,
    (acc: ColumnWidths, col: TimeSeriesValue, c: number) => {
      const isLabel =
        (verticalTimeAxis && isTopRow) || (!verticalTimeAxis && c === 0)

      const foundField =
        isLabel && _.isString(col)
          ? fieldOptions.find(field => field.internalName === col)
          : null

      let colValue = `${col}`
      if (foundField && foundField.displayName) {
        colValue = foundField.displayName
      } else if (_.isNumber(col) && decimalPlaces.isEnforced) {
        colValue = col.toFixed(decimalPlaces.digits)
      }

      const columnLabel = topRow[c]

      const useTimeWidth =
        (columnLabel === DEFAULT_TIME_FIELD.internalName &&
          verticalTimeAxis &&
          !isTopRow) ||
        (!verticalTimeAxis &&
          isTopRow &&
          topRow[0] === DEFAULT_TIME_FIELD.internalName &&
          c !== 0)

      const currentWidth = useTimeWidth
        ? timeFormatWidth
        : calculateSize(colValue, {
            font: isLabel ? '"Roboto"' : '"RobotoMono", monospace',
            fontSize: '13px',
            fontWeight: 'bold',
          }).width + CELL_HORIZONTAL_PADDING

      const {widths: Widths} = maxColumnWidths
      const maxWidth = _.get(Widths, `${columnLabel}`, 0)

      if (isTopRow || currentWidth > maxWidth) {
        acc.widths[columnLabel] = currentWidth
        acc.totalWidths += currentWidth - maxWidth
      }

      return acc
    },
    {...maxColumnWidths}
  )

  return maxWidths
}

export const computeFieldOptions = (
  existingFieldOptions: FieldOption[],
  sortedLabels: SortedLabel[]
): FieldOption[] => {
  const timeField =
    existingFieldOptions.find(f => f.internalName === 'time') ||
    DEFAULT_TIME_FIELD
  let astNames = [timeField]
  sortedLabels.forEach(({label}) => {
    const field: TimeField = {
      internalName: label,
      displayName: '',
      visible: true,
    }
    astNames = [...astNames, field]
  })

  const intersection = existingFieldOptions.filter(f => {
    return astNames.find(a => a.internalName === f.internalName)
  })

  const newFields = astNames.filter(a => {
    return !existingFieldOptions.find(f => f.internalName === a.internalName)
  })

  return [...intersection, ...newFields]
}

export const calculateColumnWidths = (
  data: TimeSeriesValue[][],
  fieldOptions: FieldOption[],
  timeFormat: string,
  verticalTimeAxis: boolean,
  decimalPlaces: DecimalPlaces
): ColumnWidths => {
  const timeFormatWidth = calculateTimeColumnWidth(
    timeFormat === '' ? DEFAULT_TIME_FORMAT : timeFormat
  )
  return fastReduce<TimeSeriesValue[], ColumnWidths>(
    data,
    (acc: ColumnWidths, row: TimeSeriesValue[], r: number) => {
      return updateMaxWidths(
        row,
        acc,
        data[0],
        r === 0,
        fieldOptions,
        timeFormatWidth,
        verticalTimeAxis,
        decimalPlaces
      )
    },
    {widths: {}, totalWidths: 0}
  )
}

export const filterTableColumns = (
  data: TimeSeriesValue[][],
  fieldOptions: FieldOption[]
): TimeSeriesValue[][] => {
  const visibility = {}
  const filteredData = fastMap<TimeSeriesValue[], TimeSeriesValue[]>(
    data,
    (row, i) => {
      return fastFilter<TimeSeriesValue>(row, (col, j) => {
        if (i === 0) {
          const foundField = fieldOptions.find(
            field => field.internalName === col
          )
          visibility[j] = foundField ? foundField.visible : true
        }
        return visibility[j]
      })
    }
  )
  return filteredData[0].length ? filteredData : [[]]
}

export const orderTableColumns = (
  data: TimeSeriesValue[][],
  fieldOptions: FieldOption[]
): TimeSeriesValue[][] => {
  const fieldsSortOrder = fieldOptions.map(fieldOption => {
    return _.findIndex(data[0], dataLabel => {
      return dataLabel === fieldOption.internalName
    })
  })

  const filteredFieldSortOrder = fieldsSortOrder.filter(f => f !== -1)

  const orderedData = fastMap<TimeSeriesValue[], TimeSeriesValue[]>(
    data,
    (row: TimeSeriesValue[]): TimeSeriesValue[] => {
      return row.map((__, j, arr) => arr[filteredFieldSortOrder[j]])
    }
  )
  return orderedData[0].length ? orderedData : [[]]
}

export const sortTableData = (
  data: TimeSeriesValue[][],
  sort: Sort
): {sortedData: TimeSeriesValue[][]; sortedTimeVals: TimeSeriesValue[]} => {
  const sortIndex = _.indexOf(data[0], sort.field)
  const dataValues = _.drop(data, 1)
  const sortedData = [
    data[0],
    ..._.orderBy<TimeSeriesValue[]>(dataValues, sortIndex, [sort.direction]),
  ]
  const sortedTimeVals = fastMap<TimeSeriesValue[], TimeSeriesValue>(
    sortedData,
    (r: TimeSeriesValue[]): TimeSeriesValue => r[0]
  )
  return {sortedData, sortedTimeVals}
}

export const transformTableData = (
  data: TimeSeriesValue[][],
  sort: Sort,
  fieldOptions: FieldOption[],
  tableOptions: TableOptions,
  timeFormat: string,
  decimalPlaces: DecimalPlaces
): TransformTableDataReturnType => {
  const {verticalTimeAxis} = tableOptions

  const {sortedData, sortedTimeVals} = sortTableData(data, sort)
  const filteredData = filterTableColumns(sortedData, fieldOptions)
  const orderedData = orderTableColumns(filteredData, fieldOptions)
  const transformedData = verticalTimeAxis ? orderedData : _.unzip(orderedData)
  const columnWidths = calculateColumnWidths(
    transformedData,
    fieldOptions,
    timeFormat,
    verticalTimeAxis,
    decimalPlaces
  )

  return {transformedData, sortedTimeVals, columnWidths}
}
