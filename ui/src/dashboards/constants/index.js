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
  name: 'Name This Graph',
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

export const TEMPLATE_VARIABLE_TYPES = [
  {
    text: 'CSV',
    type: 'csv',
  },
  {
    text: 'Database',
    type: 'database',
  },
  {
    text: 'Measurement',
    type: 'measurement',
  },
  {
    text: 'Field Key',
    type: 'fieldKey',
  },
  {
    text: 'Tag Key',
    type: 'tagKey',
  },
  {
    text: 'Tag Value',
    type: 'tagValue',
  },
]
