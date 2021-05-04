import {
  Task,
  Dashboard,
  View,
  Label,
  Variable,
  RemoteDataState,
} from 'src/types'

import {DEFAULT_DASHBOARD_SORT_OPTIONS} from 'src/dashboards/constants'

export const myCell = {
  dashboardID: 'dash_1',
  id: 'cell_view_1',
  x: 0,
  y: 0,
  w: 4,
  h: 4,
  status: RemoteDataState.Done,
}

export const myDashboard: Dashboard = {
  id: 'dash_1',
  orgID: 'org_1',
  name: 'MyDashboard',
  description: '',
  cells: [myCell.id],
  labels: [],
  status: RemoteDataState.NotStarted,
  sortOptions: DEFAULT_DASHBOARD_SORT_OPTIONS,
}

export const myView: View = {
  id: 'cell_view_1',
  name: 'My Cell',
  properties: {
    shape: 'chronograf-v2',
    queries: [
      {
        text: 'v.bucket',
        editMode: 'builder',
        name: 'View Query',
        builderConfig: {
          buckets: ['bb8'],
          tags: [
            {
              key: '_measurement',
              values: ['cpu'],
              aggregateFunctionType: 'filter',
            },
            {
              key: '_field',
              values: [],
              aggregateFunctionType: 'filter',
            },
          ],
          functions: [],
          aggregateWindow: {period: 'auto'},
        },
      },
    ],
    axes: {
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
    },
    type: 'xy',
    geom: 'line',
    colors: [],
    note: '',
    showNoteWhenEmpty: false,
    xColumn: null,
    yColumn: null,
    position: 'overlaid',
  },
  status: RemoteDataState.Done,
}

export const myfavelabel: Label = {
  id: 'myfavelabel1',
  name: '1label',
  properties: {color: 'fffff', description: 'omg'},
  status: RemoteDataState.Done,
}

export const myfavetask: Task = {
  authorizationID: '037b084ed9abc000',
  every: '24h0m0s',
  flux:
    'option task = {name: "lala", every: 24h0m0s, offset: 1m0s}\n\nfrom(bucket: "defnuck")\n\t|> range(start: -task.every)',
  id: '037b0877b359a000',
  labels: [myfavelabel.id],
  name: 'lala',
  offset: '1m0s',
  org: 'org',
  orgID: '037b084ec8ebc000',
  status: 'active',
}

export const myVariable: Variable = {
  id: '039ae3b3b74b0000',
  orgID: '039aa15b38cb0000',
  name: 'beep',
  selected: null,
  arguments: {
    type: 'query',
    values: {
      query: 'f(x: v.a)',
      language: 'flux',
    },
  },
  labels: [],
  status: RemoteDataState.Done,
}
