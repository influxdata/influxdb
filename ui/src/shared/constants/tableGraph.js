export const NULL_ARRAY_INDEX = -1

export const NULL_HOVER_TIME = '0'

export const TIME_FORMAT_TOOLTIP_LINK =
  'http://momentjs.com/docs/#/parsing/string-format/'

export const DEFAULT_PRECISION = 0

export const DEFAULT_TIME_FIELD = {
  internalName: 'time',
  displayName: '',
  visible: true,
  precision: DEFAULT_PRECISION,
}

export const ASCENDING = 'asc'
export const DESCENDING = 'desc'
export const DEFAULT_SORT_DIRECTION = ASCENDING

export const DEFAULT_FIX_FIRST_COLUMN = true
export const DEFAULT_VERTICAL_TIME_AXIS = true

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
  verticalTimeAxis: DEFAULT_VERTICAL_TIME_AXIS,
  sortBy: DEFAULT_TIME_FIELD,
  wrapping: 'truncate',
  fixFirstColumn: DEFAULT_FIX_FIRST_COLUMN,
}
