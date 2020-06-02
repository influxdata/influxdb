// Types
import {Variable, RemoteDataState} from 'src/types'
import {VariableAssignment} from 'src/types/ast'
import {VariableNode} from 'src/variables/utils/hydrateVars'

export const defaultVariableAssignments: VariableAssignment[] = [
  {
    type: 'VariableAssignment',
    id: {
      type: 'Identifier',
      name: 'timeRangeStart',
    },
    init: {
      type: 'UnaryExpression',
      operator: '-',
      argument: {
        type: 'DurationLiteral',
        values: [{magnitude: 1, unit: 'h'}],
      },
    },
  },
  {
    type: 'VariableAssignment',
    id: {
      type: 'Identifier',
      name: 'timeRangeStop',
    },
    init: {
      type: 'CallExpression',
      callee: {type: 'Identifier', name: 'now'},
    },
  },
  {
    type: 'VariableAssignment',
    id: {
      type: 'Identifier',
      name: 'createdVariable',
    },
    init: {
      type: 'StringLiteral',
      value: 'randomValue',
    },
  },
]

export const createVariable = (
  name: string,
  query: string,
  selected?: string
): Variable => ({
  name,
  id: name,
  orgID: 'howdy',
  selected: selected ? [selected] : [],
  labels: [],
  arguments: {
    type: 'query',
    values: {
      query,
      language: 'flux',
    },
  },
  status: RemoteDataState.Done,
})

export const createMapVariable = (
  name: string,
  map: {[key: string]: string} = {},
  selected?: string
): Variable => ({
  name,
  id: name,
  orgID: 'howdy',
  selected: selected ? [selected] : [],
  labels: [],
  arguments: {
    type: 'map',
    values: {...map},
  },
  status: RemoteDataState.Done,
})

export const defaultSubGraph: VariableNode[] = [
  {
    variable: {
      id: '05b740973c68e000',
      orgID: '05b740945a91b000',
      name: 'static',
      description: '',
      selected: ['defbuck'],
      arguments: {
        type: 'constant',
        values: ['beans', 'defbuck'],
      },
      createdAt: '2020-05-19T06:00:00.113169-07:00',
      updatedAt: '2020-05-19T06:00:00.113169-07:00',
      labels: [],
      links: {
        self: '/api/v2/variables/05b740973c68e000',
        labels: '/api/v2/variables/05b740973c68e000/labels',
        org: '/api/v2/orgs/05b740945a91b000',
      },
      status: RemoteDataState.Done,
    },
    values: null,
    parents: [
      {
        variable: {
          id: '05b740974228e000',
          orgID: '05b740945a91b000',
          name: 'dependent',
          description: '',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                'from(bucket: v.static)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "test")\n  |> keep(columns: ["container_name"])\n  |> rename(columns: {"container_name": "_value"})\n  |> last()\n  |> group()',
              language: 'flux',
              results: [],
            },
          },
          createdAt: '2020-05-19T06:00:00.136597-07:00',
          updatedAt: '2020-05-19T06:00:00.136597-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05b740974228e000',
            labels: '/api/v2/variables/05b740974228e000/labels',
            org: '/api/v2/orgs/05b740945a91b000',
          },
          status: RemoteDataState.Loading,
        },
        cancel: () => {},
        values: null,
        parents: [],
        children: [
          {
            variable: {
              orgID: '',
              id: 'timeRangeStart',
              name: 'timeRangeStart',
              arguments: {
                type: 'system',
                values: [
                  [
                    {
                      magnitude: 1,
                      unit: 'h',
                    },
                  ],
                ],
              },
              status: RemoteDataState.Done,
              labels: [],
              selected: [],
            },
            values: null,
            parents: [null],
            children: [],
            status: RemoteDataState.Done,
            cancel: () => {},
          },
          {
            variable: {
              orgID: '',
              id: 'timeRangeStop',
              name: 'timeRangeStop',
              arguments: {
                type: 'system',
                values: ['now()'],
              },
              status: RemoteDataState.Done,
              labels: [],
              selected: [],
            },
            values: null,
            parents: [null],
            children: [],
            status: RemoteDataState.Done,
            cancel: () => {},
          },
        ],
        status: RemoteDataState.NotStarted,
      },
    ],
    children: [],
    status: RemoteDataState.Done,
    cancel: () => {},
  },
]

export const defaultGraph: VariableNode[] = [
  {
    variable: {
      id: '05b740973c68e000',
      orgID: '05b740945a91b000',
      name: 'static',
      description: '',
      selected: ['defbuck'],
      arguments: {
        type: 'constant',
        values: ['beans', 'defbuck'],
      },
      createdAt: '2020-05-19T06:00:00.113169-07:00',
      updatedAt: '2020-05-19T06:00:00.113169-07:00',
      labels: [],
      links: {
        self: '/api/v2/variables/05b740973c68e000',
        labels: '/api/v2/variables/05b740973c68e000/labels',
        org: '/api/v2/orgs/05b740945a91b000',
      },
      status: RemoteDataState.Done,
    },
    values: null,
    parents: [
      {
        cancel: () => {},
        variable: {
          id: '05b740974228e000',
          orgID: '05b740945a91b000',
          name: 'dependent',
          description: '',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                'from(bucket: v.static)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "test")\n  |> keep(columns: ["container_name"])\n  |> rename(columns: {"container_name": "_value"})\n  |> last()\n  |> group()',
              language: 'flux',
              results: [],
            },
          },
          createdAt: '2020-05-19T06:00:00.136597-07:00',
          updatedAt: '2020-05-19T06:00:00.136597-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05b740974228e000',
            labels: '/api/v2/variables/05b740974228e000/labels',
            org: '/api/v2/orgs/05b740945a91b000',
          },
          status: RemoteDataState.Loading,
        },
        values: null,
        parents: [],
        children: [
          {
            variable: {
              orgID: '',
              id: 'timeRangeStart',
              name: 'timeRangeStart',
              arguments: {
                type: 'system',
                values: [
                  [
                    {
                      magnitude: 1,
                      unit: 'h',
                    },
                  ],
                ],
              },
              status: RemoteDataState.Done,
              labels: [],
              selected: [],
            },
            values: null,
            parents: [null],
            children: [],
            status: RemoteDataState.Done,
            cancel: () => {},
          },
          {
            variable: {
              orgID: '',
              id: 'timeRangeStop',
              name: 'timeRangeStop',
              arguments: {
                type: 'system',
                values: ['now()'],
              },
              status: RemoteDataState.Done,
              labels: [],
              selected: [],
            },
            values: null,
            parents: [null],
            children: [],
            status: RemoteDataState.Done,
            cancel: () => {},
          },
        ],
        status: RemoteDataState.NotStarted,
      },
    ],
    children: [],
    status: RemoteDataState.Done,
    cancel: () => {},
  },
  {
    variable: {
      id: '05b740974228e000',
      orgID: '05b740945a91b000',
      name: 'dependent',
      description: '',
      selected: [],
      arguments: {
        type: 'query',
        values: {
          query:
            'from(bucket: v.static)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "test")\n  |> keep(columns: ["container_name"])\n  |> rename(columns: {"container_name": "_value"})\n  |> last()\n  |> group()',
          language: 'flux',
          results: [],
        },
      },
      createdAt: '2020-05-19T06:00:00.136597-07:00',
      updatedAt: '2020-05-19T06:00:00.136597-07:00',
      labels: [],
      links: {
        self: '/api/v2/variables/05b740974228e000',
        labels: '/api/v2/variables/05b740974228e000/labels',
        org: '/api/v2/orgs/05b740945a91b000',
      },
      status: RemoteDataState.Loading,
    },
    values: null,
    parents: [],
    children: [
      {
        variable: {
          id: '05b740973c68e000',
          orgID: '05b740945a91b000',
          name: 'static',
          description: '',
          selected: ['defbuck'],
          arguments: {
            type: 'constant',
            values: ['beans', 'defbuck'],
          },
          createdAt: '2020-05-19T06:00:00.113169-07:00',
          updatedAt: '2020-05-19T06:00:00.113169-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05b740973c68e000',
            labels: '/api/v2/variables/05b740973c68e000/labels',
            org: '/api/v2/orgs/05b740945a91b000',
          },
          status: RemoteDataState.Done,
        },
        values: null,
        parents: [null],
        children: [],
        status: RemoteDataState.Done,
        cancel: () => {},
      },
      {
        variable: {
          orgID: '',
          id: 'timeRangeStart',
          name: 'timeRangeStart',
          arguments: {
            type: 'system',
            values: [
              [
                {
                  magnitude: 1,
                  unit: 'h',
                },
              ],
            ],
          },
          status: RemoteDataState.Done,
          labels: [],
          selected: [],
        },
        values: null,
        parents: [null],
        children: [],
        status: RemoteDataState.Done,
        cancel: () => {},
      },
      {
        variable: {
          orgID: '',
          id: 'timeRangeStop',
          name: 'timeRangeStop',
          arguments: {
            type: 'system',
            values: ['now()'],
          },
          status: RemoteDataState.Done,
          labels: [],
          selected: [],
        },
        values: null,
        parents: [null],
        children: [],
        status: RemoteDataState.Done,
        cancel: () => {},
      },
    ],
    status: RemoteDataState.NotStarted,
    cancel: () => {},
  },
  {
    variable: {
      orgID: '',
      id: 'timeRangeStart',
      name: 'timeRangeStart',
      arguments: {
        type: 'system',
        values: [
          [
            {
              magnitude: 1,
              unit: 'h',
            },
          ],
        ],
      },
      status: RemoteDataState.Done,
      labels: [],
      selected: [],
    },
    values: null,
    parents: [
      {
        variable: {
          id: '05b740974228e000',
          orgID: '05b740945a91b000',
          name: 'dependent',
          description: '',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                'from(bucket: v.static)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "test")\n  |> keep(columns: ["container_name"])\n  |> rename(columns: {"container_name": "_value"})\n  |> last()\n  |> group()',
              language: 'flux',
              results: [],
            },
          },
          createdAt: '2020-05-19T06:00:00.136597-07:00',
          updatedAt: '2020-05-19T06:00:00.136597-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05b740974228e000',
            labels: '/api/v2/variables/05b740974228e000/labels',
            org: '/api/v2/orgs/05b740945a91b000',
          },
          status: RemoteDataState.Loading,
        },
        values: null,
        parents: [],
        children: [
          {
            variable: {
              id: '05b740973c68e000',
              orgID: '05b740945a91b000',
              name: 'static',
              description: '',
              selected: ['defbuck'],
              arguments: {
                type: 'constant',
                values: ['beans', 'defbuck'],
              },
              createdAt: '2020-05-19T06:00:00.113169-07:00',
              updatedAt: '2020-05-19T06:00:00.113169-07:00',
              labels: [],
              links: {
                self: '/api/v2/variables/05b740973c68e000',
                labels: '/api/v2/variables/05b740973c68e000/labels',
                org: '/api/v2/orgs/05b740945a91b000',
              },
              status: RemoteDataState.Done,
            },
            values: null,
            parents: [null],
            children: [],
            status: RemoteDataState.Done,
            cancel: () => {},
          },
          {
            variable: {
              orgID: '',
              id: 'timeRangeStop',
              name: 'timeRangeStop',
              arguments: {
                type: 'system',
                values: ['now()'],
              },
              status: RemoteDataState.Done,
              labels: [],
              selected: [],
            },
            values: null,
            parents: [null],
            children: [],
            status: RemoteDataState.Done,
            cancel: () => {},
          },
        ],
        status: RemoteDataState.NotStarted,
        cancel: () => {},
      },
    ],
    children: [],
    status: RemoteDataState.Done,
    cancel: () => {},
  },
  {
    variable: {
      orgID: '',
      id: 'timeRangeStop',
      name: 'timeRangeStop',
      arguments: {
        type: 'system',
        values: ['now()'],
      },
      status: RemoteDataState.Done,
      labels: [],
      selected: [],
    },
    values: null,
    parents: [
      {
        variable: {
          id: '05b740974228e000',
          orgID: '05b740945a91b000',
          name: 'dependent',
          description: '',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                'from(bucket: v.static)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "test")\n  |> keep(columns: ["container_name"])\n  |> rename(columns: {"container_name": "_value"})\n  |> last()\n  |> group()',
              language: 'flux',
              results: [],
            },
          },
          createdAt: '2020-05-19T06:00:00.136597-07:00',
          updatedAt: '2020-05-19T06:00:00.136597-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05b740974228e000',
            labels: '/api/v2/variables/05b740974228e000/labels',
            org: '/api/v2/orgs/05b740945a91b000',
          },
          status: RemoteDataState.Loading,
        },
        values: null,
        parents: [],
        children: [
          {
            variable: {
              id: '05b740973c68e000',
              orgID: '05b740945a91b000',
              name: 'static',
              description: '',
              selected: ['defbuck'],
              arguments: {
                type: 'constant',
                values: ['beans', 'defbuck'],
              },
              createdAt: '2020-05-19T06:00:00.113169-07:00',
              updatedAt: '2020-05-19T06:00:00.113169-07:00',
              labels: [],
              links: {
                self: '/api/v2/variables/05b740973c68e000',
                labels: '/api/v2/variables/05b740973c68e000/labels',
                org: '/api/v2/orgs/05b740945a91b000',
              },
              status: RemoteDataState.Done,
            },
            values: null,
            parents: [null],
            children: [],
            status: RemoteDataState.Done,
            cancel: () => {},
          },
          {
            variable: {
              orgID: '',
              id: 'timeRangeStart',
              name: 'timeRangeStart',
              arguments: {
                type: 'system',
                values: [
                  [
                    {
                      magnitude: 1,
                      unit: 'h',
                    },
                  ],
                ],
              },
              status: RemoteDataState.Done,
              labels: [],
              selected: [],
            },
            values: null,
            parents: [null],
            children: [],
            status: RemoteDataState.Done,
            cancel: () => {},
          },
          null,
        ],
        status: RemoteDataState.NotStarted,
        cancel: () => {},
      },
    ],
    children: [],
    status: RemoteDataState.Done,
    cancel: () => {},
  },
  {
    variable: {
      orgID: '',
      id: 'windowPeriod',
      name: 'windowPeriod',
      arguments: {
        type: 'system',
        values: [10000],
      },
      status: RemoteDataState.Done,
      labels: [],
      selected: [],
    },
    values: null,
    parents: [],
    children: [],
    status: RemoteDataState.Done,
    cancel: () => {},
  },
]

export const defaultVariable: Variable = {
  id: '05b73f4bffe8e000',
  orgID: '05b73f49a1d1b000',
  name: 'static',
  description: 'defaultVariable',
  selected: ['defbuck'],
  arguments: {type: 'constant', values: ['beans', 'defbuck']},
  createdAt: '2020-05-19T05:54:20.927477-07:00',
  updatedAt: '2020-05-19T05:54:20.927477-07:00',
  labels: [],
  links: {
    self: '/api/v2/variables/05b73f4bffe8e000',
    labels: '/api/v2/variables/05b73f4bffe8e000/labels',
    org: '/api/v2/orgs/05b73f49a1d1b000',
  },
  status: RemoteDataState.Done,
}

export const associatedVariable: Variable = {
  id: '05b740974228e000',
  orgID: '05b740945a91b000',
  name: 'dependent',
  description: 'associatedVariable',
  selected: [],
  arguments: {
    type: 'query',
    values: {
      query:
        'from(bucket: v.static)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "test")\n  |> keep(columns: ["container_name"])\n  |> rename(columns: {"container_name": "_value"})\n  |> last()\n  |> group()',
      language: 'flux',
      results: [],
    },
  },
  createdAt: '2020-05-19T06:00:00.136597-07:00',
  updatedAt: '2020-05-19T06:00:00.136597-07:00',
  labels: [],
  links: {
    self: '/api/v2/variables/05b740974228e000',
    labels: '/api/v2/variables/05b740974228e000/labels',
    org: '/api/v2/orgs/05b740945a91b000',
  },
  status: RemoteDataState.Loading,
}

export const timeRangeStartVariable: Variable = {
  orgID: '',
  id: 'timeRangeStart',
  name: 'timeRangeStart',
  arguments: {
    type: 'system',
    values: [
      [
        {
          magnitude: 1,
          unit: 'h',
        },
      ],
    ],
  },
  status: RemoteDataState.Done,
  labels: [],
  selected: [],
}

export const timeRangeStopVariable: Variable = {
  orgID: '',
  id: 'timeRangeStop',
  name: 'timeRangeStop',
  arguments: {
    type: 'system',
    values: ['now()'],
  },
  status: RemoteDataState.Done,
  labels: [],
  selected: [],
}

export const windowPeriodVariable: Variable = {
  orgID: '',
  id: 'windowPeriod',
  name: 'windowPeriod',
  arguments: {
    type: 'system',
    values: [10000],
  },
  status: RemoteDataState.Done,
  labels: [],
  selected: [],
}

export const defaultVariables: Variable[] = [
  defaultVariable,
  associatedVariable,
  timeRangeStartVariable,
  timeRangeStopVariable,
  windowPeriodVariable,
]
