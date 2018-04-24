export const NULL_ARRAY_INDEX = -1

export const NULL_HOVER_TIME = '0'

export const TIME_FORMAT_TOOLTIP_LINK =
  'http://momentjs.com/docs/#/parsing/string-format/'

export const TIME_FIELD_DEFAULT = {
  internalName: 'time',
  displayName: '',
  visible: true,
}

export const ASCENDING = 'asc'
export const DESCENDING = 'desc'
export const DEFAULT_SORT_DIRECTION = ASCENDING

export const FIX_FIRST_COLUMN_DEFAULT = true
export const VERTICAL_TIME_AXIS_DEFAULT = true

export const CELL_HORIZONTAL_PADDING = 30

export const DEFAULT_TIME_FORMAT = 'MM/DD/YYYY HH:mm:ss'
export const TIME_FORMAT_CUSTOM = 'Custom'

export const FORMAT_OPTIONS = [
  {text: DEFAULT_TIME_FORMAT},
  {text: 'MM/DD/YYYY HH:mm:ss.SSS'},
  {text: 'YYYY-MM-DD HH:mm:ss'},
  {text: 'HH:mm:ss'},
  {text: 'HH:mm:ss.SSS'},
  {text: 'MMMM D, YYYY HH:mm:ss'},
  {text: 'dddd, MMMM D, YYYY HH:mm:ss'},
  {text: TIME_FORMAT_CUSTOM},
]

export const DEFAULT_TABLE_OPTIONS = {
  verticalTimeAxis: VERTICAL_TIME_AXIS_DEFAULT,
  timeFormat: DEFAULT_TIME_FORMAT,
  sortBy: TIME_FIELD_DEFAULT,
  wrapping: 'truncate',
  fixFirstColumn: FIX_FIRST_COLUMN_DEFAULT,
}
