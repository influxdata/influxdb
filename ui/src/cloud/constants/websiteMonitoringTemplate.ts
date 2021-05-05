import {DashboardTemplate} from 'src/types'

export default {
  meta: {
    version: '1',
    type: 'dashboard',
    name: 'Website Monitoring Demo Data Dashboard Template',
    description:
      'template created from dashboard: Website Monitoring Demo Data',
  },
  content: {
    data: {
      type: 'dashboard',
      attributes: {
        name: 'Website Monitoring Demo Data Dashboard',
        description: 'Visualize Website Monitoring Demo Data',
      },
      relationships: {
        cell: {
          data: [
            {
              type: 'cell',
              id: '04e6ec89904e8000',
            },
            {
              type: 'cell',
              id: '04e6ec8996118000',
            },
            {
              type: 'cell',
              id: '04e6ec8996bd6000',
            },
            {
              type: 'cell',
              id: '04e6ec8997359000',
            },
            {
              type: 'cell',
              id: '04e6ec8999905000',
            },
            {
              type: 'cell',
              id: '04e6ec899938a000',
            },
            {
              type: 'cell',
              id: '04e6ec899ca1c000',
            },
            {
              type: 'cell',
              id: '04e6ec899c91f000',
            },
            {
              type: 'cell',
              id: '04e6ec899dbe5000',
            },
            {
              type: 'cell',
              id: '04e6ec899ec13000',
            },
            {
              type: 'cell',
              id: '04e6ec89a08dd000',
            },
          ],
        },
        variable: {
          data: [],
        },
      },
    },
    included: [
      {
        id: '04e6ec89904e8000',
        type: 'cell',
        attributes: {
          x: 2,
          y: 1,
          w: 5,
          h: 2,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec89904e8000',
            },
          },
        },
      },
      {
        id: '04e6ec8996118000',
        type: 'cell',
        attributes: {
          x: 0,
          y: 6,
          w: 12,
          h: 4,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec8996118000',
            },
          },
        },
      },
      {
        id: '04e6ec8996bd6000',
        type: 'cell',
        attributes: {
          x: 9,
          y: 4,
          w: 3,
          h: 2,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec8996bd6000',
            },
          },
        },
      },
      {
        id: '04e6ec8997359000',
        type: 'cell',
        attributes: {
          x: 0,
          y: 1,
          w: 2,
          h: 2,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec8997359000',
            },
          },
        },
      },
      {
        id: '04e6ec8999905000',
        type: 'cell',
        attributes: {
          x: 7,
          y: 1,
          w: 2,
          h: 2,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec8999905000',
            },
          },
        },
      },
      {
        id: '04e6ec899938a000',
        type: 'cell',
        attributes: {
          x: 0,
          y: 3,
          w: 12,
          h: 1,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec899938a000',
            },
          },
        },
      },
      {
        id: '04e6ec899ca1c000',
        type: 'cell',
        attributes: {
          x: 0,
          y: 4,
          w: 2,
          h: 2,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec899ca1c000',
            },
          },
        },
      },
      {
        id: '04e6ec899c91f000',
        type: 'cell',
        attributes: {
          x: 2,
          y: 4,
          w: 5,
          h: 2,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec899c91f000',
            },
          },
        },
      },
      {
        id: '04e6ec899dbe5000',
        type: 'cell',
        attributes: {
          x: 9,
          y: 1,
          w: 3,
          h: 2,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec899dbe5000',
            },
          },
        },
      },
      {
        id: '04e6ec899ec13000',
        type: 'cell',
        attributes: {
          x: 7,
          y: 4,
          w: 2,
          h: 2,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec899ec13000',
            },
          },
        },
      },
      {
        id: '04e6ec89a08dd000',
        type: 'cell',
        attributes: {
          x: 0,
          y: 0,
          w: 12,
          h: 1,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '04e6ec89a08dd000',
            },
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec89904e8000',
        attributes: {
          name: 'Response Time',
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "response_time")\n  |> filter(fn: (r) => r.server == "https://influxdata.com") \n  |> drop(columns: ["_start", "_stop","_measurement","method","result","server","status_code"])',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
                },
              },
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "response_time")\n  |> filter(fn: (r) => r.server == "https://influxdata.com")\n  |> map(fn: (r) => ({ r with _value: 1.0 }))\n  |> set(key: "_field", value: "1 Second")\n  |> keep(columns: ["_time", "_value","_field"])',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
                },
              },
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "response_time")\n  |> filter(fn: (r) => r.server == "https://influxdata.com")\n  |> map(fn: (r) => ({ r with _value: 0.5 }))\n  |> set(key: "_field", value: "0.5 Seconds")\n  |> keep(columns: ["_time", "_value","_field"])',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
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
            type: 'line-plus-single-stat',
            colors: [
              {
                id: 'base',
                type: 'text',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
              {
                id: 'ed8d4d40-1c61-41e7-9224-f5f9f048e46a',
                type: 'text',
                hex: '#FFB94A',
                name: 'pineapple',
                value: 1,
              },
              {
                id: '4cc90c18-70c9-4851-bdc2-5db54f0fc6c1',
                type: 'text',
                hex: '#BF3D5E',
                name: 'ruby',
                value: 5,
              },
            ],
            prefix: '',
            suffix: 's',
            decimalPlaces: {
              isEnforced: true,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
            xColumn: '_time',
            yColumn: '_value',
            shadeBelow: false,
            position: '',
            hoverDimension: 'y',
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec8996118000',
        attributes: {
          name: 'Response Log',
          properties: {
            shape: 'chronograf-v2',
            type: 'table',
            queries: [
              {
                text:
                  'import "influxdata/influxdb/v1"\nfrom(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> v1.fieldsAsCols()\n  |> group(columns: ["method", "server"], mode:"by")\n  |> drop(columns: ["_start", "_stop","result","_measurement","host","result_type","result_code"])\n  |> group()',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
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
                id: 'base',
                type: 'text',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
            ],
            tableOptions: {
              verticalTimeAxis: true,
              sortBy: {
                internalName: '',
                displayName: '',
                visible: false,
              },
              wrapping: '',
              fixFirstColumn: false,
            },
            fieldOptions: [
              {
                internalName: '',
                displayName: '',
                visible: true,
              },
              {
                internalName: 'result',
                displayName: 'result',
                visible: true,
              },
              {
                internalName: 'table',
                displayName: 'table',
                visible: true,
              },
              {
                internalName: '_start',
                displayName: '_start',
                visible: true,
              },
              {
                internalName: '_stop',
                displayName: '_stop',
                visible: true,
              },
              {
                internalName: '_time',
                displayName: '_time',
                visible: true,
              },
              {
                internalName: '_measurement',
                displayName: '_measurement',
                visible: true,
              },
              {
                internalName: 'host',
                displayName: 'host',
                visible: true,
              },
              {
                internalName: 'method',
                displayName: 'method',
                visible: true,
              },
              {
                internalName: 'result',
                displayName: 'result',
                visible: true,
              },
              {
                internalName: 'server',
                displayName: 'server',
                visible: true,
              },
              {
                internalName: 'http_response_code',
                displayName: 'http_response_code',
                visible: true,
              },
              {
                internalName: 'result_type',
                displayName: 'result_type',
                visible: true,
              },
              {
                internalName: 'content_length',
                displayName: 'content_length',
                visible: true,
              },
              {
                internalName: 'response_time',
                displayName: 'response_time',
                visible: true,
              },
              {
                internalName: 'result_code',
                displayName: 'result_code',
                visible: true,
              },
            ],
            timeFormat: 'YYYY-MM-DD HH:mm:ss',
            decimalPlaces: {
              isEnforced: false,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec8996bd6000',
        attributes: {
          name: 'Response Code Counts',
          properties: {
            shape: 'chronograf-v2',
            type: 'table',
            queries: [
              {
                text:
                  'import "influxdata/influxdb/v1"\nfrom(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "http_response_code")\n  |> filter(fn: (r) => r.server == "https://docs.influxdata.com")\n  |> duplicate(column: "_value", as: "http_response_code")\n  |> group(columns: ["server","http_response_code"])\n  |> count(column: "_value")\n  |> drop(columns: ["server"])\n  |> rename(columns: {http_response_code:"Response Code", _value: "Count"})\n',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
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
                id: 'base',
                type: 'text',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
            ],
            tableOptions: {
              verticalTimeAxis: true,
              sortBy: {
                internalName: '',
                displayName: '',
                visible: false,
              },
              wrapping: '',
              fixFirstColumn: false,
            },
            fieldOptions: [
              {
                internalName: '',
                displayName: '',
                visible: true,
              },
              {
                internalName: 'result',
                displayName: 'result',
                visible: true,
              },
              {
                internalName: 'table',
                displayName: 'table',
                visible: true,
              },
              {
                internalName: 'Response Code',
                displayName: 'Response Code',
                visible: true,
              },
              {
                internalName: 'Count',
                displayName: 'Count',
                visible: true,
              },
            ],
            timeFormat: 'YYYY-MM-DD HH:mm:ss',
            decimalPlaces: {
              isEnforced: false,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec8997359000',
        attributes: {
          name: 'Last Status Code',
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "http_response_code")\n  |> filter(fn: (r) => r.server == "https://influxdata.com")\n  |> last()',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
                },
              },
            ],
            prefix: '',
            tickPrefix: '',
            suffix: '',
            tickSuffix: '',
            colors: [
              {
                id: 'base',
                type: 'background',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
              {
                id: 'dad182b1-537c-4bc1-9e7a-b016fa9de070',
                type: 'background',
                hex: '#FFD255',
                name: 'thunder',
                value: 300,
              },
              {
                id: 'e1a12ab5-35e0-4553-a2d5-bf8db3b0f6df',
                type: 'background',
                hex: '#F95F53',
                name: 'curacao',
                value: 400,
              },
              {
                id: 'e07775ef-1975-43b7-a59f-2d2180dc1d3b',
                type: 'background',
                hex: '#BF3D5E',
                name: 'ruby',
                value: 600,
              },
            ],
            decimalPlaces: {
              isEnforced: true,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec8999905000',
        attributes: {
          name: 'Max Response Time',
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "response_time")\n  |> filter(fn: (r) => r.server == "https://influxdata.com")\n  |> max()\n  ',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
                },
              },
            ],
            prefix: '',
            tickPrefix: '',
            suffix: 's',
            tickSuffix: '',
            colors: [
              {
                id: 'base',
                type: 'text',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
            ],
            decimalPlaces: {
              isEnforced: true,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec899938a000',
        attributes: {
          name: 'Name this Cell',
          properties: {
            shape: 'chronograf-v2',
            type: 'markdown',
            note: '## *https://docs.influxdata.com*',
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec899ca1c000',
        attributes: {
          name: 'Last Status Code',
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "http_response_code")\n  |> filter(fn: (r) => r.server == "https://docs.influxdata.com")\n  |> last()',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
                },
              },
            ],
            prefix: '',
            tickPrefix: '',
            suffix: '',
            tickSuffix: '',
            colors: [
              {
                id: 'base',
                type: 'background',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
              {
                id: 'dad182b1-537c-4bc1-9e7a-b016fa9de070',
                type: 'background',
                hex: '#FFD255',
                name: 'thunder',
                value: 300,
              },
              {
                id: 'e1a12ab5-35e0-4553-a2d5-bf8db3b0f6df',
                type: 'background',
                hex: '#F95F53',
                name: 'curacao',
                value: 400,
              },
              {
                id: 'e07775ef-1975-43b7-a59f-2d2180dc1d3b',
                type: 'background',
                hex: '#BF3D5E',
                name: 'ruby',
                value: 600,
              },
            ],
            decimalPlaces: {
              isEnforced: true,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec899c91f000',
        attributes: {
          name: 'Response Time',
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "response_time")\n  |> filter(fn: (r) => r.server == "https://docs.influxdata.com")\n  |> drop(columns: ["_start", "_stop","_measurement","method","result","server","status_code"])',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
                },
              },
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "response_time")\n  |> filter(fn: (r) => r.server == "https://docs.influxdata.com")\n  |> map(fn: (r) => ({ r with _value: 1.0 }))\n  |> set(key: "_field", value: "1 Second")\n  |> keep(columns: ["_time", "_value","_field"])',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
                },
              },
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "response_time")\n  |> filter(fn: (r) => r.server == "https://docs.influxdata.com")\n  |> map(fn: (r) => ({ r with _value: 0.5 }))\n  |> set(key: "_field", value: "0.5 Seconds")\n  |> keep(columns: ["_time", "_value","_field"])',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
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
            type: 'line-plus-single-stat',
            colors: [
              {
                id: 'base',
                type: 'text',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
            ],
            prefix: '',
            suffix: 's',
            decimalPlaces: {
              isEnforced: true,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
            xColumn: '_time',
            yColumn: '_value',
            shadeBelow: false,
            position: '',
            hoverDimension: 'y',
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec899dbe5000',
        attributes: {
          name: 'Response Code Counts',
          properties: {
            shape: 'chronograf-v2',
            type: 'table',
            queries: [
              {
                text:
                  'import "influxdata/influxdb/v1"\nfrom(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "http_response_code")\n  |> filter(fn: (r) => r.server == "https://influxdata.com")\n  |> duplicate(column: "_value", as: "http_response_code")\n  |> group(columns: ["server","http_response_code"])\n  |> count(column: "_value")\n  |> drop(columns: ["server"])\n  |> rename(columns: {http_response_code:"Response Code", _value: "Count"})\n',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
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
                id: 'base',
                type: 'text',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
            ],
            tableOptions: {
              verticalTimeAxis: true,
              sortBy: {
                internalName: '',
                displayName: '',
                visible: false,
              },
              wrapping: '',
              fixFirstColumn: false,
            },
            fieldOptions: [
              {
                internalName: '',
                displayName: '',
                visible: true,
              },
              {
                internalName: 'result',
                displayName: 'result',
                visible: true,
              },
              {
                internalName: 'table',
                displayName: 'table',
                visible: true,
              },
              {
                internalName: 'Response Code',
                displayName: 'Response Code',
                visible: true,
              },
              {
                internalName: 'Count',
                displayName: 'Count',
                visible: true,
              },
            ],
            timeFormat: 'YYYY-MM-DD HH:mm:ss',
            decimalPlaces: {
              isEnforced: false,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec899ec13000',
        attributes: {
          name: 'Max Response Time',
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text:
                  'from(bucket: "Website Monitoring Bucket")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "http_response")\n  |> filter(fn: (r) => r._field == "response_time")\n  |> filter(fn: (r) => r.server == "https://docs.influxdata.com")\n  |> max()\n  ',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [
                    {
                      key: '_measurement',
                      values: [],
                    },
                  ],
                  functions: [],
                  aggregateWindow: {
                    period: 'auto',
                  },
                },
              },
            ],
            prefix: '',
            tickPrefix: '',
            suffix: 's',
            tickSuffix: '',
            colors: [
              {
                id: 'base',
                type: 'text',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
            ],
            decimalPlaces: {
              isEnforced: true,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
          },
        },
      },
      {
        type: 'view',
        id: '04e6ec89a08dd000',
        attributes: {
          name: 'Name this Cell',
          properties: {
            shape: 'chronograf-v2',
            type: 'markdown',
            note: '## *https://influxdata.com*',
          },
        },
      },
    ],
  },
  labels: [],
} as DashboardTemplate
