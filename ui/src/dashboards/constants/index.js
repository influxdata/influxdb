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

export const TEMPLATE_VARIBALE_TYPES = [
  {
    text: 'CSV',
    type: 'csv',
  },
  {
    text: 'Measurements',
    type: 'measurements',
  },
  {
    text: 'Databases',
    type: 'databases',
  },
  {
    text: 'Fields',
    type: 'fields',
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
