import calculateSize from 'calculate-size'
import _ from 'lodash'
import {map, reduce, filter} from 'fast.js'

import {
  CELL_HORIZONTAL_PADDING,
  TIME_FIELD_DEFAULT,
  TIME_FORMAT_DEFAULT,
} from 'src/shared/constants/tableGraph'

const calculateTimeColumnWidth = timeFormat => {
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
  row,
  maxColumnWidths,
  topRow,
  isTopRow,
  fieldNames,
  timeFormatWidth,
  verticalTimeAxis
) => {
  return reduce(
    row,
    (acc, col, c) => {
      const isLabel =
        (verticalTimeAxis && isTopRow) || (!verticalTimeAxis && c === 0)

      const foundField = isLabel
        ? fieldNames.find(field => field.internalName === col)
        : undefined
      const colValue =
        foundField && foundField.displayName ? foundField.displayName : `${col}`
      const columnLabel = topRow[c]

      const useTimeWidth =
        (columnLabel === TIME_FIELD_DEFAULT.internalName &&
          verticalTimeAxis &&
          !isTopRow) ||
        (!verticalTimeAxis &&
          isTopRow &&
          topRow[0] === TIME_FIELD_DEFAULT.internalName &&
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
}

export const computeFieldNames = (existingFieldNames, sortedLabels) => {
  const timeField =
    existingFieldNames.find(f => f.internalName === 'time') ||
    TIME_FIELD_DEFAULT
  let astNames = [timeField]

  sortedLabels.forEach(({label}) => {
    const field = {internalName: label, displayName: '', visible: true}
    astNames = [...astNames, field]
  })

  const intersection = existingFieldNames.filter(f => {
    return astNames.find(a => a.internalName === f.internalName)
  })

  const newFields = astNames.filter(a => {
    return !existingFieldNames.find(f => f.internalName === a.internalName)
  })

  return [...intersection, ...newFields]
}

export const calculateColumnWidths = (
  data,
  fieldNames,
  timeFormat,
  verticalTimeAxis
) => {
  const timeFormatWidth = calculateTimeColumnWidth(
    timeFormat === '' ? TIME_FORMAT_DEFAULT : timeFormat
  )
  return reduce(
    data,
    (acc, row, r) => {
      return updateMaxWidths(
        row,
        acc,
        data[0],
        r === 0,
        fieldNames,
        timeFormatWidth,
        verticalTimeAxis
      )
    },
    {widths: {}, totalWidths: 0}
  )
}

export const filterTableColumns = (data, fieldNames) => {
  const visibility = {}
  const filteredData = map(data, (row, i) => {
    return filter(row, (col, j) => {
      if (i === 0) {
        const foundField = fieldNames.find(field => field.internalName === col)
        visibility[j] = foundField ? foundField.visible : true
      }
      return visibility[j]
    })
  })
  return filteredData[0].length ? filteredData : [[]]
}

export const orderTableColumns = (data, fieldNames) => {
  const fieldsSortOrder = fieldNames.map(fieldName => {
    return _.findIndex(data[0], dataLabel => {
      return dataLabel === fieldName.internalName
    })
  })
  const filteredFieldSortOrder = filter(fieldsSortOrder, f => f !== -1)
  const orderedData = map(data, row => {
    return row.map((v, j, arr) => arr[filteredFieldSortOrder[j]] || v)
  })
  return orderedData[0].length ? orderedData : [[]]
}

export const transformTableData = (data, sort, fieldNames, tableOptions) => {
  const {verticalTimeAxis, timeFormat} = tableOptions
  const sortIndex = _.indexOf(data[0], sort.field)
  const sortedData = [
    data[0],
    ..._.orderBy(_.drop(data, 1), sortIndex, [sort.direction]),
  ]
  const sortedTimeVals = map(sortedData, r => r[0])
  const filteredData = filterTableColumns(sortedData, fieldNames)
  const orderedData = orderTableColumns(filteredData, fieldNames)
  const transformedData = verticalTimeAxis ? orderedData : _.unzip(orderedData)
  const {widths: columnWidths, totalWidths} = calculateColumnWidths(
    transformedData,
    fieldNames,
    timeFormat,
    verticalTimeAxis
  )
  return {transformedData, sortedTimeVals, columnWidths, totalWidths}
}
