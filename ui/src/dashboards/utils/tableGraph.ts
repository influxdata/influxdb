import calculateSize from 'calculate-size'
import _ from 'lodash'
import {map, reduce, filter} from 'fast.js'

import {
  CELL_HORIZONTAL_PADDING,
  TIME_FIELD_DEFAULT,
  DEFAULT_TIME_FORMAT,
  DEFAULT_PRECISION,
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
  fieldOptions,
  timeFormatWidth,
  verticalTimeAxis
) => {
  return reduce(
    row,
    (acc, col, c) => {
      const isLabel =
        (verticalTimeAxis && isTopRow) || (!verticalTimeAxis && c === 0)

      const foundField = isLabel
        ? fieldOptions.find(field => field.internalName === col)
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

export const computeFieldOptions = (existingFieldOptions, sortedLabels) => {
  const timeField =
    existingFieldOptions.find(f => f.internalName === 'time') ||
    TIME_FIELD_DEFAULT
  let astNames = [timeField]

  sortedLabels.forEach(({label}) => {
    const field = {
      internalName: label,
      displayName: '',
      visible: true,
      precision: DEFAULT_PRECISION,
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
  data,
  fieldOptions,
  timeFormat,
  verticalTimeAxis
) => {
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
        verticalTimeAxis
      )
    },
    {widths: {}, totalWidths: 0}
  )
}

export const filterTableColumns = (data, fieldOptions) => {
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

export const orderTableColumns = (data, fieldOptions) => {
  const fieldsSortOrder = fieldOptions.map(fieldOption => {
    return _.findIndex(data[0], dataLabel => {
      return dataLabel === fieldOption.internalName
    })
  })
  const filteredFieldSortOrder = filter(fieldsSortOrder, f => f !== -1)
  const orderedData = map(data, row => {
    return row.map((v, j, arr) => arr[filteredFieldSortOrder[j]] || v)
  })
  return orderedData[0].length ? orderedData : [[]]
}

export const transformTableData = (
  data,
  sort,
  fieldOptions,
  tableOptions,
  timeFormat
) => {
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
    verticalTimeAxis
  )
  return {transformedData, sortedTimeVals, columnWidths, totalWidths}
}
