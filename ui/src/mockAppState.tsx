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
          name: 'bucket',
          selected: ['Homeward Bound'],
          arguments: {
            type: 'query',
            values: {
              query:
                '// buckets\nbuckets()\n  |> filter(fn: (r) => r.name !~ /^_/)\n  |> rename(columns: {name: "_value"})\n  |> keep(columns: ["_value"])\n',
              language: 'flux',
            },
          },
        },
        '05782ef09ddb8000': {
          id: '05782ef09ddb8000',
          name: 'base_query',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// base_query\nfrom(bucket: v.bucket)\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "cpu")\n  |> filter(fn: (r) => r._field == "usage_user")',
              language: 'flux',
            },
          },
        },
        '05aeb0ad75aca000': {
          id: '05aeb0ad75aca000',
          name: 'values',
          selected: ['system'],
          arguments: {
            type: 'map',
            values: {
              system: 'system',
              usage_user: 'usage_user',
            },
          },
        },
        '05ba3253105a5000': {
          id: '05ba3253105a5000',
          name: 'broker_host',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// broker_host\nimport "influxdata/influxdb/v1"\nv1.tagValues(bucket: v.bucket, tag: "host")',
              language: 'flux',
            },
          },
        },
        '05e6e4df2287b000': {
          id: '05e6e4df2287b000',
          name: 'deployment',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// deployment\nimport "influxdata/influxdb/v1"\nv1.tagValues(bucket: v.bucket, tag: "cpu") |> keep(columns: ["_value"])',
              language: 'flux',
            },
          },
        },
        '05e6e4fb0887b000': {
          id: '05e6e4fb0887b000',
          name: 'build',
          selected: [],
          arguments: {
            type: 'query',
            values: {
              query:
                '// build\nimport "influxdata/influxdb/v1"\nimport "strings"\n\nv1.tagValues(bucket: v.bucket, tag: "cpu") |> filter(fn: (r) => strings.hasSuffix(v: r._value, suffix: v.deployment))',
              language: 'flux',
            },
          },
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
