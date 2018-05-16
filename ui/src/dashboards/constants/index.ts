import {
  DEFAULT_VERTICAL_TIME_AXIS,
  DEFAULT_FIX_FIRST_COLUMN,
} from 'src/shared/constants/tableGraph'
import {Cell, QueryConfig} from 'src/types'
import {CellType, Dashboard, DecimalPlaces} from 'src/types/dashboard'
import {TEMP_VAR_DASHBOARD_TIME} from 'src/shared/constants'

export const UNTITLED_GRAPH: string = 'Untitled Graph'

export const TIME_FORMAT_TOOLTIP_LINK: string =
  'http://momentjs.com/docs/#/parsing/string-format/'

export const DEFAULT_DECIMAL_PLACES: DecimalPlaces = {
  isEnforced: false,
  digits: 3,
}

export const DEFAULT_TIME_FIELD = {
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

type NewDefaultCell = Pick<
  Cell,
  Exclude<keyof Cell, 'id' | 'axes' | 'colors' | 'links' | 'legend'>
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

export const NEW_DASHBOARD = {
  name: 'Name This Dashboard',
  cells: [NEW_DEFAULT_DASHBOARD_CELL],
}

export const TEMPLATE_TYPES = [
  {
    text: 'CSV',
    type: 'csv',
  },
  {
    text: 'Databases',
    type: 'databases',
  },
  {
    text: 'Measurements',
    type: 'measurements',
  },
  {
    text: 'Field Keys',
    type: 'fieldKeys',
  },
  {
    text: 'Tag Keys',
    type: 'tagKeys',
  },
  {
    text: 'Tag Values',
    type: 'tagValues',
  },
]

export const TEMPLATE_VARIABLE_TYPES = {
  csv: 'csv',
  databases: 'database',
  measurements: 'measurement',
  fieldKeys: 'fieldKey',
  tagKeys: 'tagKey',
  tagValues: 'tagValue',
}

export const TEMPLATE_VARIABLE_QUERIES = {
  databases: 'SHOW DATABASES',
  measurements: 'SHOW MEASUREMENTS ON :database:',
  fieldKeys: 'SHOW FIELD KEYS ON :database: FROM :measurement:',
  tagKeys: 'SHOW TAG KEYS ON :database: FROM :measurement:',
  tagValues:
    'SHOW TAG VALUES ON :database: FROM :measurement: WITH KEY=:tagKey:',
}

export const MATCH_INCOMPLETE_TEMPLATES = /:[\w-]*/g

export const applyMasks = query => {
  const matchWholeTemplates = /:([\w-]*):/g
  const maskForWholeTemplates = '😸$1😸'
  return query.replace(matchWholeTemplates, maskForWholeTemplates)
}
export const insertTempVar = (query, tempVar) => {
  return query.replace(MATCH_INCOMPLETE_TEMPLATES, tempVar)
}
export const unMask = query => {
  return query.replace(/😸/g, ':')
}
export const removeUnselectedTemplateValues = templates => {
  return templates.map(template => {
    const selectedValues = template.values.filter(value => value.selected)
    return {...template, values: selectedValues}
  })
}

export const TYPE_QUERY_CONFIG = 'queryConfig'
export const TYPE_SHIFTED = 'shifted queryConfig'
export const TYPE_IFQL = 'ifql'
export const DASHBOARD_NAME_MAX_LENGTH = 50
export const TEMPLATE_RANGE = {upper: null, lower: TEMP_VAR_DASHBOARD_TIME}
