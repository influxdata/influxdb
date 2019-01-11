import {TimeField} from 'src/dashboards/constants'

export const NULL_ARRAY_INDEX = -1

export const ASCENDING = 'asc'
export const DESCENDING = 'desc'
export const DEFAULT_SORT_DIRECTION = ASCENDING

export const DEFAULT_FIX_FIRST_COLUMN = true
export const DEFAULT_VERTICAL_TIME_AXIS = true

export const CELL_HORIZONTAL_PADDING = 30

export const DEFAULT_TIME_FIELD: TimeField = {
  internalName: '_time',
  displayName: 'time',
  visible: true,
}
