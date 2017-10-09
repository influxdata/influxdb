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

export const DISPLAY_OPTIONS = {
  LINEAR: 'linear',
  LOG: 'log',
  BASE_2: '2',
  BASE_10: '10',
}

export const TOOLTIP_CONTENT = {
  FORMAT:
    '<p><strong>K/M/B</strong> = Thousand / Million / Billion<br/><strong>K/M/G</strong> = Kilo / Mega / Giga </p>',
}

export const TYPE_QUERY_CONFIG = 'queryConfig'
export const TYPE_IFQL = 'ifql'
