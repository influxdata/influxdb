export const NULL_COLUMN_INDEX = -1
export const NULL_ROW_INDEX = -1

export const NULL_HOVER_TIME = '0'

export const TIME_FORMAT_DEFAULT = 'MM/DD/YYYY HH:mm:ss.ss'
export const TIME_FORMAT_CUSTOM = 'Custom'

export const TIME_FIELD_DEFAULT = {
  internalName: 'time',
  displayName: '',
  visible: true,
}

export const ASCENDING = 'asc'
export const DESCENDING = 'desc'
export const FIX_FIRST_COLUMN_DEFAULT = true

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
  sortBy: TIME_FIELD_DEFAULT,
  wrapping: 'truncate',
  fieldNames: [TIME_FIELD_DEFAULT],
  fixFirstColumn: FIX_FIRST_COLUMN_DEFAULT,
}
