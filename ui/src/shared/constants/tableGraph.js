import calculateSize from 'calculate-size'
import _ from 'lodash'

export const NULL_COLUMN_INDEX = -1
export const NULL_ROW_INDEX = -1

export const NULL_HOVER_TIME = '0'

export const TIME_FORMAT_DEFAULT = 'MM/DD/YYYY HH:mm:ss.ss'
export const TIME_FORMAT_CUSTOM = 'Custom'

export const TIME_COLUMN_DEFAULT = {internalName: 'time', displayName: ''}

export const ASCENDING = 'asc'
export const DESCENDING = 'desc'
export const FIX_FIRST_COLUMN_DEFAULT = true

export const CELL_HORIZONTAL_PADDING = 18

export const FORMAT_OPTIONS = [
  {text: TIME_FORMAT_DEFAULT},
  {text: 'MM/DD/YYYY HH:mm'},
  {text: 'MM/DD/YYYY'},
  {text: 'h:mm:ss A'},
  {text: 'h:mm A'},
  {text: 'MMMM D, YYYY'},
  {text: 'MMMM D, YYYY h:mm A'},
  {text: 'dddd, MMMM D, YYYY h:mm A'},
  {text: TIME_FORMAT_CUSTOM},
]

export const DEFAULT_TABLE_OPTIONS = {
  verticalTimeAxis: true,
  timeFormat: TIME_FORMAT_DEFAULT,
  sortBy: TIME_COLUMN_DEFAULT,
  wrapping: 'truncate',
  columnNames: [TIME_COLUMN_DEFAULT],
  fixFirstColumn: FIX_FIRST_COLUMN_DEFAULT,
}

export const calculateTimeColumnWidth = timeFormat => {
  // Force usage of longest format names for ideal measurement
  timeFormat = _.replace(timeFormat, 'MMMM', 'September')
  timeFormat = _.replace(timeFormat, 'dddd', 'Wednesday')
  timeFormat = _.replace(timeFormat, 'A', 'AM')
  timeFormat = _.replace(timeFormat, 'h', '00')

  const {width} = calculateSize(timeFormat, {
    font: '"RobotoMono", monospace',
    fontSize: '13px',
    fontWeight: 'bold',
  })

  return width + CELL_HORIZONTAL_PADDING
}
