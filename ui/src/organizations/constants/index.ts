export const MIN_RETENTION_SECONDS = 3600

export const systemTemplate = (bucketName: string) => ({
  meta: {
    version: '1',
    name: 'System-Template',
    description: 'dashboard template for the system telegraf plugin',
  },
  content: {
    data: {
      type: 'dashboard',
      attributes: {
        name: 'System',
        description:
          'A collection of useful visualizations for monitoring your system stats',
      },
      relationships: {
        label: {
          data: [],
        },
        cell: {
          data: [
            {
              type: 'cell',
              id: '0387c87bb5b3e001',
            },
            {
              type: 'cell',
              id: '0387c87bb5f3e000',
            },
            {
              type: 'cell',
              id: '0387c87bb5f3e001',
            },
            {
              type: 'cell',
              id: '0387c87bb633e000',
            },
            {
              type: 'cell',
              id: '0387c87bb633e001',
            },
            {
              type: 'cell',
              id: '0387c87bb633e002',
            },
            {
              type: 'cell',
              id: '0387c87bb673e000',
            },
            {
              type: 'cell',
              id: '0387c87bb673e001',
            },
            {
              type: 'cell',
              id: '0387c87bb6b3e000',
            },
            {
              type: 'cell',
              id: '0387c87bb6b3e001',
            },
            {
              type: 'cell',
              id: '0387c87bb6f3e000',
            },
            {
              type: 'cell',
              id: '0387c87bb733e000',
            },
            {
              type: 'cell',
              id: '0387c87bb733e001',
            },
          ],
        },
      },
    },
    included: [
      {
        id: '0387c87bb5b3e001',
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
              id: '0387c87bb5b3e001',
            },
          },
        },
      },
      {
        id: '0387c87bb5f3e000',
        type: 'cell',
        attributes: {
          x: 0,
          y: 1,
          w: 3,
          h: 1,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb5f3e000',
            },
          },
        },
      },
      {
        id: '0387c87bb5f3e001',
        type: 'cell',
        attributes: {
          x: 3,
          y: 1,
          w: 2,
          h: 1,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb5f3e001',
            },
          },
        },
      },
      {
        id: '0387c87bb633e000',
        type: 'cell',
        attributes: {
          x: 5,
          y: 1,
          w: 2,
          h: 1,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb633e000',
            },
          },
        },
      },
      {
        id: '0387c87bb633e001',
        type: 'cell',
        attributes: {
          x: 7,
          y: 1,
          w: 2,
          h: 1,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb633e001',
            },
          },
        },
      },
      {
        id: '0387c87bb633e002',
        type: 'cell',
        attributes: {
          x: 0,
          y: 2,
          w: 3,
          h: 3,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb633e002',
            },
          },
        },
      },
      {
        id: '0387c87bb673e000',
        type: 'cell',
        attributes: {
          x: 3,
          y: 2,
          w: 3,
          h: 3,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb673e000',
            },
          },
        },
      },
      {
        id: '0387c87bb673e001',
        type: 'cell',
        attributes: {
          x: 6,
          y: 2,
          w: 3,
          h: 3,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb673e001',
            },
          },
        },
      },
      {
        id: '0387c87bb6b3e000',
        type: 'cell',
        attributes: {
          x: 9,
          y: 1,
          w: 3,
          h: 4,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb6b3e000',
            },
          },
        },
      },
      {
        id: '0387c87bb6b3e001',
        type: 'cell',
        attributes: {
          x: 0,
          y: 5,
          w: 3,
          h: 3,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb6b3e001',
            },
          },
        },
      },
      {
        id: '0387c87bb6f3e000',
        type: 'cell',
        attributes: {
          x: 3,
          y: 5,
          w: 3,
          h: 3,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb6f3e000',
            },
          },
        },
      },
      {
        id: '0387c87bb733e000',
        type: 'cell',
        attributes: {
          x: 6,
          y: 5,
          w: 3,
          h: 3,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb733e000',
            },
          },
        },
      },
      {
        id: '0387c87bb733e001',
        type: 'cell',
        attributes: {
          x: 9,
          y: 5,
          w: 3,
          h: 3,
        },
        relationships: {
          view: {
            data: {
              type: 'view',
              id: '0387c87bb733e001',
            },
          },
        },
      },
      {
        type: 'view',
        id: '0387c87bb5b3e001',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            type: 'markdown',
            note:
              'This dashboard gives you an overview of System metrics with metrics from `system`, `mem`, `diskio`, `swap` and `net` measurements. See the [Telegraf Documentation](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/system) for help configuring these plugins.',
          },
          name: 'Name this Cell',
        },
      },
      {
        type: 'view',
        id: '0387c87bb5f3e000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "system")\n  |> filter(fn: (r) => r._field == "uptime")\n  |> window(period: 1h)\n  |> last()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> map(fn: (r) => r._value / 86400, mergeKey: true)\n  |> yield(name: "last")\n  \n  \n  `,
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['system'],
                    },
                    {
                      key: '_field',
                      values: ['uptime'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'last',
                    },
                  ],
                },
              },
            ],
            prefix: '',
            suffix: ' days',
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
              isEnforced: false,
              digits: 2,
            },
            note: '',
            showNoteWhenEmpty: false,
          },
          name: 'System Uptime',
        },
      },
      {
        type: 'view',
        id: '0387c87bb5f3e001',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "system")\n  |> filter(fn: (r) => r._field == "n_cpus")\n  |> window(period: windowPeriod)\n  |> last()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "last")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['system'],
                    },
                    {
                      key: '_field',
                      values: ['n_cpus'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'last',
                    },
                  ],
                },
              },
            ],
            prefix: '',
            suffix: ' cpus',
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
          name: 'nCPUs',
        },
      },
      {
        type: 'view',
        id: '0387c87bb633e000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "system")\n  |> filter(fn: (r) => r._field == "load1")\n  |> window(period: windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['system'],
                    },
                    {
                      key: '_field',
                      values: ['load1'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'mean',
                    },
                  ],
                },
              },
            ],
            prefix: '',
            suffix: '',
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
          name: 'System Load',
        },
      },
      {
        type: 'view',
        id: '0387c87bb633e001',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "mem")\n  |> filter(fn: (r) => r._field == "total")\n  |> window(period: windowPeriod)\n  |> last()\n  |> map(fn: (r) => r._value / 1024 / 1024 / 1024, mergeKey: true)\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "last")\n  `,
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['mem'],
                    },
                    {
                      key: '_field',
                      values: ['total'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'last',
                    },
                  ],
                },
              },
            ],
            prefix: '',
            suffix: ' GB',
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
          name: 'Total Memory',
        },
      },
      {
        type: 'view',
        id: '0387c87bb633e002',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "disk")\n  |> filter(fn: (r) => r._field == "used_percent")\n  |> window(period: windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['disk'],
                    },
                    {
                      key: '_field',
                      values: ['used_percent'],
                    },
                    {
                      key: 'fstype',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'mean',
                    },
                  ],
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
                suffix: '%',
                base: '10',
                scale: 'linear',
              },
              y2: {
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
          },
          name: 'Disk Usage',
        },
      },
      {
        type: 'view',
        id: '0387c87bb673e000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "cpu")\n  |> filter(fn: (r) => r._field == "usage_user" or r._field == "usage_system" or r._field == "usage_idle")\n  |> filter(fn: (r) => r.cpu == "cpu-total")\n  |> window(period: windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['cpu'],
                    },
                    {
                      key: '_field',
                      values: ['usage_user', 'usage_system', 'usage_idle'],
                    },
                    {
                      key: 'cpu',
                      values: ['cpu-total'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'mean',
                    },
                  ],
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
                suffix: '%',
                base: '10',
                scale: 'linear',
              },
              y2: {
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
          },
          name: 'CPU Usage',
        },
      },
      {
        type: 'view',
        id: '0387c87bb673e001',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "system")\n  |> filter(fn: (r) => r._field == "load1" or r._field == "load5" or r._field == "load15")\n  |> window(period: windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['system'],
                    },
                    {
                      key: '_field',
                      values: ['load1', 'load5', 'load15'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'mean',
                    },
                  ],
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
                label: 'Load',
                prefix: '',
                suffix: '',
                base: '10',
                scale: 'linear',
              },
              y2: {
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
          },
          name: 'System Load',
        },
      },
      {
        type: 'view',
        id: '0387c87bb6b3e000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "mem")\n  |> filter(fn: (r) => r._field == "used_percent")\n  |> window(period: windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['mem'],
                    },
                    {
                      key: '_field',
                      values: ['used_percent'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'mean',
                    },
                  ],
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
                suffix: '%',
                base: '10',
                scale: 'linear',
              },
              y2: {
                bounds: ['', ''],
                label: '',
                prefix: '',
                suffix: '',
                base: '10',
                scale: 'linear',
              },
            },
            type: 'line-plus-single-stat',
            legend: {},
            colors: [
              {
                id: 'base',
                type: 'text',
                hex: '#00C9FF',
                name: 'laser',
                value: 0,
              },
              {
                id: 'c2f922df-60a1-4471-91fc-c16427e7fcfb',
                type: 'scale',
                hex: '#8F8AF4',
                name: 'Do Androids Dream of Electric Sheep?',
                value: 0,
              },
              {
                id: '330f7fee-d44e-4a15-b2d6-2330178ec203',
                type: 'scale',
                hex: '#A51414',
                name: 'Do Androids Dream of Electric Sheep?',
                value: 0,
              },
              {
                id: 'e3c73eb3-665a-414b-afdd-1686c9b962d9',
                type: 'scale',
                hex: '#F4CF31',
                name: 'Do Androids Dream of Electric Sheep?',
                value: 0,
              },
            ],
            prefix: '',
            suffix: '%',
            decimalPlaces: {
              isEnforced: true,
              digits: 1,
            },
            note: '',
            showNoteWhenEmpty: false,
          },
          name: 'Memory Usage',
        },
      },
      {
        type: 'view',
        id: '0387c87bb6b3e001',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "diskio")\n  |> filter(fn: (r) => r._field == "read_bytes" or r._field == "write_bytes")\n  |> derivative(unit: windowPeriod, nonNegative: false)\n  |> yield(name: "derivative")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['diskio'],
                    },
                    {
                      key: '_field',
                      values: ['read_bytes', 'write_bytes'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'derivative',
                    },
                  ],
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
                label: 'Bytes',
                prefix: '',
                suffix: '',
                base: '10',
                scale: 'linear',
              },
              y2: {
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
          },
          name: 'Disk IO',
        },
      },
      {
        type: 'view',
        id: '0387c87bb6f3e000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "net")\n  |> filter(fn: (r) => r._field == "bytes_recv" or r._field == "bytes_sent")\n  |> derivative(unit: windowPeriod, nonNegative: false)\n  |> yield(name: "derivative")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['net'],
                    },
                    {
                      key: '_field',
                      values: ['bytes_recv', 'bytes_sent'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'derivative',
                    },
                  ],
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
                label: 'Bytes',
                prefix: '',
                suffix: '',
                base: '10',
                scale: 'linear',
              },
              y2: {
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
          },
          name: 'Network',
        },
      },
      {
        type: 'view',
        id: '0387c87bb733e000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "processes")\n  |> filter(fn: (r) => r._field == "running" or r._field == "blocked" or r._field == "idle" or r._field == "unknown")\n  |> window(period: windowPeriod)\n  |> max()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "max")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['processes'],
                    },
                    {
                      key: '_field',
                      values: ['running', 'blocked', 'idle', 'unknown'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'max',
                    },
                  ],
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
              y2: {
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
          },
          name: 'Processes',
        },
      },
      {
        type: 'view',
        id: '0387c87bb733e001',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text: `from(bucket: "${bucketName}")\n  |> range(start: timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "swap")\n  |> filter(fn: (r) => r._field == "total" or r._field == "used")\n  |> window(period: windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")`,
                editMode: 'builder',
                name: '',
                builderConfig: {
                  buckets: [bucketName],
                  tags: [
                    {
                      key: '_measurement',
                      values: ['swap'],
                    },
                    {
                      key: '_field',
                      values: ['total', 'used'],
                    },
                    {
                      key: 'host',
                      values: [],
                    },
                  ],
                  functions: [
                    {
                      name: 'mean',
                    },
                  ],
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
              y2: {
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
          },
          name: 'Swap',
        },
      },
    ],
  },
  labels: [],
})
