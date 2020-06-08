import {
  Axes,
  Cell,
  Color,
  Dashboard,
  FieldOption,
  DecimalPlaces,
  TableOptions,
  RemoteDataState,
} from 'src/types'

import {DEFAULT_DASHBOARD_SORT_OPTIONS} from 'src/dashboards/constants'

export const dbCell: Cell = {
  x: 1,
  y: 2,
  w: 3,
  h: 4,
  id: '1',
  dashboardID: '1',
  status: RemoteDataState.Done,
  links: {
    self: '/v2/dashboards/1/cells/1',
    view: '/v2/dashboards/1/cells/1/views',
  },
}

export const dashboard: Dashboard = {
  id: '1',
  name: 'd1',
  orgID: '1',
  cells: [dbCell.id],
  status: RemoteDataState.Done,
  labels: [],
  links: {
    self: '/v2/dashboards/1',
    cells: '/v2/dashboards/cells',
  },
  sortOptions: DEFAULT_DASHBOARD_SORT_OPTIONS,
}

export const axes: Axes = {
  x: {
    bounds: ['', ''],
    label: '',
    prefix: '',
    suffix: '',
    base: '10',
    scale: 'linear',
  },
  y: {
    bounds: ['', ''],
    label: '',
    prefix: '',
    suffix: '',
    base: '10',
    scale: 'linear',
  },
}

export const fieldOptions: FieldOption[] = [
  {
    internalName: 'time',
    displayName: '',
    visible: true,
  },
]

export const tableOptions: TableOptions = {
  verticalTimeAxis: true,
  sortBy: {
    internalName: 'time',
    displayName: '',
    visible: true,
  },
  wrapping: 'truncate',
  fixFirstColumn: true,
}
export const lineColors: Color[] = [
  {
    id: '574fb0a3-0a26-44d7-8d71-d4981756acb1',
    type: 'scale',
    hex: '#31C0F6',
    name: 'Nineteen Eighty Four',
    value: 0,
  },
  {
    id: '3b9750f9-d41d-4100-8ee6-bd2785237f35',
    type: 'scale',
    hex: '#A500A5',
    name: 'Nineteen Eighty Four',
    value: 0,
  },
  {
    id: '8d39064f-8124-4967-ae22-ffe14e425781',
    type: 'scale',
    hex: '#FF7E27',
    name: 'Nineteen Eighty Four',
    value: 0,
  },
]

export const decimalPlaces: DecimalPlaces = {
  isEnforced: true,
  digits: 4,
}

export const cell: Cell = {
  id: '67435af2-17bf-4caa-a5fc-0dd1ffb40dab',
  dashboardID: 'DashboardID',
  x: 0,
  y: 0,
  w: 8,
  h: 4,
  links: {
    self: '/chronograf/v1/dashboards/9/cells/67435',
    view: '1',
  },
  status: RemoteDataState.Done,
}

export const fullTimeRange = {
  dashboardID: 9,
  defaultGroupBy: '10s',
  seconds: 300,
  inputValue: 'Past 5 minutes',
  lower: 'now() - 5m',
  upper: null,
  menuOption: 'Past 5 minutes',
  format: 'influxql',
}

export const thresholdsListColors: Color[] = [
  {
    type: 'text',
    hex: '#00C9FF',
    id: 'base',
    name: 'laser',
    value: -1000000000000000000,
  },
]

export const gaugeColors: Color[] = [
  {
    type: 'min',
    hex: '#00C9FF',
    id: '0',
    name: 'laser',
    value: 0,
  },
  {
    type: 'max',
    hex: '#9394FF',
    id: '1',
    name: 'comet',
    value: 100,
  },
]
