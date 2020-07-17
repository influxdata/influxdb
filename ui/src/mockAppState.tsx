import {RemoteDataState} from 'src/types'

export const getMockAppState = (query?: string, variables?: []) => {
  const newAppState = Object.assign({}, mockAppState)
  if (query) {
    newAppState.timeMachines.timeMachines.de.view.properties.queries[0].text = query
    newAppState.timeMachines.timeMachines.de.draftQueries[0].text = query
  }
  if (variables && variables.length > 0) {
    // TODO: update variables
  }
  return newAppState
}

export const mockAppState = {
  app: {
    persisted: {
      timeZone: 'Local',
    },
  },
  links: {
    query: {
      ast: '/api/v2/query/ast',
      self: '/api/v2/query',
      suggestions: '/api/v2/query/suggestions',
    },
  },
  currentDashboard: {
    id: '',
  },
  resources: {
    orgs: {
      org: {
        id: '674b23253171ee69',
      },
    },
    variables: {
      byID: {
        '054b7476389f1000': {
          id: '054b7476389f1000',
          orgID: '674b23253171ee69',
          name: 'bucket',
          description: '',
          selected: ['Homeward Bound'],
          arguments: {
            type: 'query',
            values: {
              query:
                '// buckets\nbuckets()\n  |> filter(fn: (r) => r.name !~ /^_/)\n  |> rename(columns: {name: "_value"})\n  |> keep(columns: ["_value"])\n',
              language: 'flux',
            },
          },
          createdAt: '2020-02-25T11:30:40.482278-08:00',
          updatedAt: '2020-07-15T14:28:39.240725-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/054b7476389f1000',
            labels: '/api/v2/variables/054b7476389f1000/labels',
            org: '/api/v2/orgs/674b23253171ee69',
          },
          status: RemoteDataState.NotStarted,
        },
        '05782ef09ddb8000': {
          id: '05782ef09ddb8000',
          orgID: '674b23253171ee69',
          name: 'base_query',
          description: '',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// base_query\nfrom(bucket: v.bucket)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "cpu")\n  |> filter(fn: (r) => r._field == "usage_user")',
              language: 'flux',
            },
          },
          createdAt: '2020-03-31T06:18:34.615076-07:00',
          updatedAt: '2020-07-15T14:28:26.685766-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05782ef09ddb8000',
            labels: '/api/v2/variables/05782ef09ddb8000/labels',
            org: '/api/v2/orgs/674b23253171ee69',
          },
          status: RemoteDataState.NotStarted,
        },
        '05aeb0ad75aca000': {
          id: '05aeb0ad75aca000',
          orgID: '674b23253171ee69',
          name: 'values',
          description: '',
          selected: ['system'],
          arguments: {
            type: 'map',
            values: {
              system: 'system',
              usage_user: 'usage_user',
            },
          },
          createdAt: '2020-05-12T14:23:23.222747-07:00',
          updatedAt: '2020-05-12T14:23:23.222747-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05aeb0ad75aca000',
            labels: '/api/v2/variables/05aeb0ad75aca000/labels',
            org: '/api/v2/orgs/674b23253171ee69',
          },
          status: RemoteDataState.Done,
        },
        '05ba3253105a5000': {
          id: '05ba3253105a5000',
          orgID: '674b23253171ee69',
          name: 'broker_host',
          description: '',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// broker_host\nimport "influxdata/influxdb/v1"\nv1.tagValues(bucket: v.bucket, tag: "host")',
              language: 'flux',
            },
          },
          createdAt: '2020-05-21T12:53:06.881887-07:00',
          updatedAt: '2020-07-15T14:28:33.980146-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05ba3253105a5000',
            labels: '/api/v2/variables/05ba3253105a5000/labels',
            org: '/api/v2/orgs/674b23253171ee69',
          },
          status: RemoteDataState.NotStarted,
        },
        '05e6e4df2287b000': {
          id: '05e6e4df2287b000',
          orgID: '674b23253171ee69',
          name: 'deployment',
          description: '',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// deployment\nimport "influxdata/influxdb/v1"\nv1.tagValues(bucket: v.bucket, tag: "cpu") |> keep(columns: ["_value"])',
              language: 'flux',
            },
          },
          createdAt: '2020-06-25T06:06:21.962137-07:00',
          updatedAt: '2020-07-15T14:28:55.499456-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05e6e4df2287b000',
            labels: '/api/v2/variables/05e6e4df2287b000/labels',
            org: '/api/v2/orgs/674b23253171ee69',
          },
          status: RemoteDataState.NotStarted,
        },
        '05e6e4fb0887b000': {
          id: '05e6e4fb0887b000',
          orgID: '674b23253171ee69',
          name: 'build',
          description: '',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// build\nimport "influxdata/influxdb/v1"\nimport "strings"\n\nv1.tagValues(bucket: v.bucket, tag: "cpu") |> filter(fn: (r) => strings.hasSuffix(v: r._value, suffix: v.deployment))',
              language: 'flux',
            },
          },
          createdAt: '2020-06-25T06:06:50.530959-07:00',
          updatedAt: '2020-07-15T14:28:48.134806-07:00',
          labels: [],
          links: {
            self: '/api/v2/variables/05e6e4fb0887b000',
            labels: '/api/v2/variables/05e6e4fb0887b000/labels',
            org: '/api/v2/orgs/674b23253171ee69',
          },
          status: RemoteDataState.NotStarted,
        },
      },
      allIDs: [
        '054b7476389f1000',
        '05782ef09ddb8000',
        '05aeb0ad75aca000',
        '05ba3253105a5000',
        '05e6e4df2287b000',
        '05e6e4fb0887b000',
      ],
    },
  },
  timeMachines: {
    activeTimeMachineID: 'de',
    timeMachines: {
      de: {
        view: {
          name: 'Name this Cell',
          status: RemoteDataState.Done,
          properties: {
            queries: [
              {
                name: '',
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "cpu")\n  |> filter(fn: (r) => r["_field"] == "usage_user")',
                editMode: 'builder',
                builderConfig: {
                  buckets: ['Homeward Bound'],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['cpu'],
                      aggregateFunctionType: 'filter',
                    },
                    {
                      key: '_field',
                      values: ['usage_user'],
                      aggregateFunctionType: 'filter',
                    },
                    {
                      key: 'cpu',
                      values: [],
                      aggregateFunctionType: 'filter',
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
                },
              },
            ],
            colors: [
              {
                type: 'scale',
                hex: '#31C0F6',
                id: '32c3bfc0-66f5-4da3-8ef8-5749ee365247',
                name: 'Nineteen Eighty Four',
                value: 0,
              },
              {
                type: 'scale',
                hex: '#A500A5',
                id: 'bbc2059b-2eff-4840-90bb-7d6f49847343',
                name: 'Nineteen Eighty Four',
                value: 0,
              },
              {
                type: 'scale',
                hex: '#FF7E27',
                id: '15f59f8d-1305-4025-b393-6f6d641bf99f',
                name: 'Nineteen Eighty Four',
                value: 0,
              },
            ],
            legend: {},
            note: '',
            showNoteWhenEmpty: false,
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
            shape: 'chronograf-v2',
            geom: 'line',
            xColumn: null,
            yColumn: null,
            position: 'overlaid',
          },
        },
        draftQueries: [
          {
            text:
              'from(bucket: "v.bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "cpu")\n  |> filter(fn: (r) => r["_field"] == "usage_user")',
          },
        ],
        activeTab: 'queries',
        activeQueryIndex: 0,
      },
    },
  },
}
