import {DEFAULT_TABLE_OPTIONS} from 'src/shared/constants/tableGraph'

export const EMPTY_DASHBOARD = {
  id: 0,
  name: '',
  cells: [
    {
      x: 0,
      y: 0,
      queries: [],
      name: 'Loading...',
      type: 'single-stat',
    },
  ],
}

export const NEW_DEFAULT_DASHBOARD_CELL = {
  x: 0,
  y: 0,
  w: 4,
  h: 4,
  name: 'Untitled Cell',
  type: 'line',
  queries: [],
  tableOptions: DEFAULT_TABLE_OPTIONS,
}

const getMostCommonValue = values => {
  const distribution = {}
  let max = 0
  let result = 0

  values.forEach(value => {
    distribution[value] = (distribution[value] || 0) + 1
    if (distribution[value] > max) {
      max = distribution[value]
      result = [value]
      return
    }
    if (distribution[value] === max) {
      result.push(value)
    }
  })

  return result[0]
}

export const generateNewDashboardCell = dashboard => {
  if (dashboard.cells.length === 0) {
    return NEW_DEFAULT_DASHBOARD_CELL
  }

  const newCellY = dashboard.cells
    .map(cell => cell.y + cell.h)
    .reduce((a, b) => (a > b ? a : b))

  const existingCellWidths = dashboard.cells.map(cell => cell.w)
  const existingCellHeights = dashboard.cells.map(cell => cell.h)

  const mostCommonCellWidth = getMostCommonValue(existingCellWidths)
  const mostCommonCellHeight = getMostCommonValue(existingCellHeights)

  return {
    ...NEW_DEFAULT_DASHBOARD_CELL,
    y: newCellY,
    w: mostCommonCellWidth,
    h: mostCommonCellHeight,
  }
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
