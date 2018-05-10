import calculateSize from 'calculate-size'
import _ from 'lodash'
import {map, reduce, filter} from 'fast.js'

import {CELL_HORIZONTAL_PADDING} from 'src/shared/constants/tableGraph'
import {DEFAULT_TIME_FIELD, DEFAULT_TIME_FORMAT} from 'src/dashboards/constants'
import {
  DbData,
  Sort,
  FieldOption,
  TableOptions,
  DecimalPlaces,
} from 'src/types/dashboard'

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

interface ColumnWidths {
  totalWidths: number
  widths: {[x: string]: number}
}
const updateMaxWidths = (
  row: DbData[],
  maxColumnWidths: ColumnWidths,
  topRow: DbData[],
  isTopRow: boolean,
  fieldOptions: FieldOption[],
  timeFormatWidth: number,
  verticalTimeAxis: boolean,
  decimalPlaces: DecimalPlaces
): ColumnWidths => {
  const maxWidths = reduce(
    row,
    (acc, col, c) => {
      const isLabel =
        (verticalTimeAxis && isTopRow) || (!verticalTimeAxis && c === 0)

      const foundField = isLabel
        ? fieldOptions.find(field => field.internalName === col)
        : undefined
      const isNumerical = _.isNumber(col)

      let colValue = `${col}`
      if (foundField && foundField.displayName) {
        colValue = foundField.displayName
      } else if (isNumerical && decimalPlaces.isEnforced) {
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

      const {widths: maxWidths} = maxColumnWidths
      const maxWidth = _.get(maxWidths, `${columnLabel}`, 0)

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

interface SortedLabel {
  label: string
  responseIndex: number
  seriesIndex: number
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
    const field = {
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
  data: DbData[][],
  fieldOptions: FieldOption[],
  timeFormat: string,
  verticalTimeAxis: boolean,
  decimalPlaces: DecimalPlaces
): ColumnWidths => {
  const timeFormatWidth = calculateTimeColumnWidth(
    timeFormat === '' ? DEFAULT_TIME_FORMAT : timeFormat
  )
  return reduce(
    data,
    (acc, row, r) => {
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
  data: DbData[][],
  fieldOptions: FieldOption[]
): DbData[][] => {
  const visibility = {}
  const filteredData = map(data, (row, i) => {
    return filter(row, (col, j) => {
      if (i === 0) {
        const foundField = fieldOptions.find(
          field => field.internalName === col
        )
        visibility[j] = foundField ? foundField.visible : true
      }
      return visibility[j]
    })
  })
  return filteredData[0].length ? filteredData : [[]]
}

export const orderTableColumns = (
  data: DbData[][],
  fieldOptions: FieldOption[]
): DbData[][] => {
  const fieldsSortOrder = fieldOptions.map(fieldOption => {
    return _.findIndex(data[0], dataLabel => {
      return dataLabel === fieldOption.internalName
    })
  })
  const filteredFieldSortOrder = filter(fieldsSortOrder, f => f !== -1)
  const orderedData = map(data, row => {
    return row.map((__, j, arr) => arr[filteredFieldSortOrder[j]])
  })
  return orderedData[0].length ? orderedData : [[]]
}

interface TransformTableDataReturnType {
  transformedData: DbData[][]
  sortedTimeVals: number[]
  columnWidths: ColumnWidths
  totalWidths: number
}
export const transformTableData = (
  data: DbData[][],
  sort: Sort,
  fieldOptions: FieldOption[],
  tableOptions: TableOptions,
  timeFormat: string,
  decimalPlaces: DecimalPlaces
): TransformTableDataReturnType => {
  const {verticalTimeAxis} = tableOptions
  const sortIndex = _.indexOf(data[0], sort.field)
  const sortedData = [
    data[0],
    ..._.orderBy(_.drop(data, 1), sortIndex, [sort.direction]),
  ]
  const sortedTimeVals = map(sortedData, r => r[0])
  const filteredData = filterTableColumns(sortedData, fieldOptions)
  const orderedData = orderTableColumns(filteredData, fieldOptions)
  const transformedData = verticalTimeAxis ? orderedData : _.unzip(orderedData)
  const {widths: columnWidths, totalWidths} = calculateColumnWidths(
    transformedData,
    fieldOptions,
    timeFormat,
    verticalTimeAxis,
    decimalPlaces
  )
  return {transformedData, sortedTimeVals, columnWidths, totalWidths}
}
