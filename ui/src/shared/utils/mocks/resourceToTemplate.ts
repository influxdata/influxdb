import {Task, Dashboard, View, Label} from 'src/types'
import {IVariable as Variable} from '@influxdata/influx'

export const myDashboard: Dashboard = {
  id: 'dash_1',
  orgID: 'org_1',
  name: 'MyDashboard',
  description: '',
  cells: [
    {
      dashboardID: 'dash_1',
      id: 'cell_view_1',
      x: 0,
      y: 0,
      w: 4,
      h: 4,
    },
  ],
  labels: [],
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
            },
            {
              key: '_field',
              values: [],
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
    legend: {},
    geom: 'line',
    colors: [],
    note: '',
    showNoteWhenEmpty: false,
    xColumn: null,
    yColumn: null,
    position: 'overlaid',
  },
}

export const myfavelabel: Label = {
  id: '1',
  name: '1label',
  properties: {color: 'fffff', description: 'omg'},
}

export const myfavetask: Task = {
  authorizationID: '037b084ed9abc000',
  every: '24h0m0s',
  flux:
    'option task = {name: "lala", every: 24h0m0s, offset: 1m0s}\n\nfrom(bucket: "defnuck")\n\t|> range(start: -task.every)',
  id: '037b0877b359a000',
  labels: [
    {
      id: '037b0c86a92a2000',
      name: 'yum',
      properties: {color: '#FF8564', description: ''},
    },
  ],
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
}
