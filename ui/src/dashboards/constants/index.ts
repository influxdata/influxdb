import {
  DEFAULT_VERTICAL_TIME_AXIS,
  DEFAULT_FIX_FIRST_COLUMN,
} from 'src/shared/constants/tableGraph'
import {Cell, QueryConfig} from 'src/types'
import {CellType, Dashboard, DecimalPlaces} from 'src/types/dashboard'

export const UNTITLED_GRAPH: string = 'Untitled Graph'

export const TIME_FORMAT_TOOLTIP_LINK: string =
  'http://momentjs.com/docs/#/parsing/string-format/'

export const DEFAULT_DECIMAL_PLACES: DecimalPlaces = {
  isEnforced: false,
  digits: 3,
}

export interface TimeField {
  internalName: string
  displayName: string
  visible: boolean
}
export const DEFAULT_TIME_FIELD: TimeField = {
  internalName: 'time',
  displayName: '',
  visible: true,
}

export const DEFAULT_TABLE_OPTIONS = {
  verticalTimeAxis: DEFAULT_VERTICAL_TIME_AXIS,
  sortBy: DEFAULT_TIME_FIELD,
  wrapping: 'truncate',
  fixFirstColumn: DEFAULT_FIX_FIRST_COLUMN,
}

export const DEFAULT_TIME_FORMAT: string = 'MM/DD/YYYY HH:mm:ss'
export const TIME_FORMAT_CUSTOM: string = 'Custom'

export const FORMAT_OPTIONS: Array<{text: string}> = [
  {text: DEFAULT_TIME_FORMAT},
  {text: 'MM/DD/YYYY HH:mm:ss.SSS'},
  {text: 'YYYY-MM-DD HH:mm:ss'},
  {text: 'HH:mm:ss'},
  {text: 'HH:mm:ss.SSS'},
  {text: 'MMMM D, YYYY HH:mm:ss'},
  {text: 'dddd, MMMM D, YYYY HH:mm:ss'},
  {text: TIME_FORMAT_CUSTOM},
]

export type NewDefaultCell = Pick<
  Cell,
  Exclude<keyof Cell, 'i' | 'axes' | 'colors' | 'links' | 'legend'>
>
export const NEW_DEFAULT_DASHBOARD_CELL: NewDefaultCell = {
  x: 0,
  y: 0,
  w: 4,
  h: 4,
  name: UNTITLED_GRAPH,
  type: CellType.Line,
  queries: [],
  tableOptions: DEFAULT_TABLE_OPTIONS,
  timeFormat: DEFAULT_TIME_FORMAT,
  decimalPlaces: DEFAULT_DECIMAL_PLACES,
  fieldOptions: [DEFAULT_TIME_FIELD],
}

interface EmptyDefaultDashboardCell {
  x: number
  y: number
  queries: QueryConfig[]
  name: string
  type: CellType
}
type EmptyDefaultDashboard = Pick<
  Dashboard,
  Exclude<keyof Dashboard, 'templates' | 'links' | 'organization' | 'cells'>
> & {
  cells: EmptyDefaultDashboardCell[]
}
export const EMPTY_DASHBOARD: EmptyDefaultDashboard = {
  id: 0,
  name: '',
  cells: [
    {
      x: 0,
      y: 0,
      queries: [],
      name: 'Loading...',
      type: CellType.Line,
    },
  ],
}

type NewDefaultDashboard = Pick<
  Dashboard,
  Exclude<keyof Dashboard, 'id' | 'templates' | 'organization' | 'cells'> & {
    cells: NewDefaultCell[]
  }
>
export const DEFAULT_DASHBOARD_NAME = 'Name This Dashboard'
export const NEW_DASHBOARD: NewDefaultDashboard = {
  name: DEFAULT_DASHBOARD_NAME,
  cells: [NEW_DEFAULT_DASHBOARD_CELL],
}

export const TYPE_QUERY_CONFIG: string = 'queryConfig'
export const TYPE_SHIFTED: string = 'shifted queryConfig'
export const TYPE_FLUX: string = 'flux'
export const DASHBOARD_NAME_MAX_LENGTH: number = 50
