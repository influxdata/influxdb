import calculateSize from 'calculate-size'
import _ from 'lodash'
import {reduce} from 'fast.js'

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
