export const NULL_COLUMN_INDEX = -1
export const NULL_ROW_INDEX = -1

export const NULL_HOVER_TIME = '0'

export const TIME_FORMAT_DEFAULT = 'MM/DD/YYYY HH:mm:ss.ss'
export const TIME_FORMAT_CUSTOM = 'Custom'

export const TIME_COLUMN_DEFAULT = {internalName: 'time', displayName: ''}

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
}
