export const systemTemplate = () => ({
  meta: {
    version: '1',
    name: 'System-Template',
    description: 'Dashboard template for the system telegraf plugin',
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
              id: '039d8c0b62c34000',
            },
            {
              type: 'cell',
              id: '039d8c0b63434000',
            },
            {
              type: 'cell',
              id: '039d8c0b63c34000',
            },
            {
              type: 'cell',
              id: '039d8c0b64034000',
            },
            {
              type: 'cell',
              id: '039d8c0b64c34000',
            },
            {
              type: 'cell',
              id: '039d8c0b65034000',
            },
            {
              type: 'cell',
              id: '039d8c0b65834000',
            },
            {
              type: 'cell',
              id: '039d8c0b66034000',
            },
            {
              type: 'cell',
              id: '039d8c0b66834000',
            },
            {
              type: 'cell',
              id: '039d8c0b67034000',
            },
            {
              type: 'cell',
              id: '039d8c0b67434000',
            },
            {
              type: 'cell',
              id: '039d8c0b67c34000',
            },
            {
              type: 'cell',
              id: '039d8c0b68434000',
            },
          ],
        },
        variable: {
          data: [
            {
              type: 'variable',
              id: '0399e8fd61294000',
            },
          ],
        },
      },
    },
    included: [
      {
        id: '039d8c0b62c34000',
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
              id: '039d8c0b62c34000',
            },
          },
        },
      },
      {
        id: '039d8c0b63434000',
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
              id: '039d8c0b63434000',
            },
          },
        },
      },
      {
        id: '039d8c0b63c34000',
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
              id: '039d8c0b63c34000',
            },
          },
        },
      },
      {
        id: '039d8c0b64034000',
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
              id: '039d8c0b64034000',
            },
          },
        },
      },
      {
        id: '039d8c0b64c34000',
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
              id: '039d8c0b64c34000',
            },
          },
        },
      },
      {
        id: '039d8c0b65034000',
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
              id: '039d8c0b65034000',
            },
          },
        },
      },
      {
        id: '039d8c0b65834000',
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
              id: '039d8c0b65834000',
            },
          },
        },
      },
      {
        id: '039d8c0b66034000',
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
              id: '039d8c0b66034000',
            },
          },
        },
      },
      {
        id: '039d8c0b66834000',
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
              id: '039d8c0b66834000',
            },
          },
        },
      },
      {
        id: '039d8c0b67034000',
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
              id: '039d8c0b67034000',
            },
          },
        },
      },
      {
        id: '039d8c0b67434000',
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
              id: '039d8c0b67434000',
            },
          },
        },
      },
      {
        id: '039d8c0b67c34000',
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
              id: '039d8c0b67c34000',
            },
          },
        },
      },
      {
        id: '039d8c0b68434000',
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
              id: '039d8c0b68434000',
            },
          },
        },
      },
      {
        type: 'view',
        id: '039d8c0b62c34000',
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
        id: '039d8c0b63434000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "system")\n  |> filter(fn: (r) => r._field == "uptime")\n  |> window(period: 1h)\n  |> last()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> map(fn: (r) => r._value / 86400, mergeKey: true)\n  |> yield(name: "last")\n  \n  \n  ',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b63c34000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "system")\n  |> filter(fn: (r) => r._field == "n_cpus")\n  |> window(period: v.windowPeriod)\n  |> last()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "last")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b64034000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "system")\n  |> filter(fn: (r) => r._field == "load1")\n  |> window(period: v.windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b64c34000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            type: 'single-stat',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "mem")\n  |> filter(fn: (r) => r._field == "total")\n  |> window(period: v.windowPeriod)\n  |> last()\n  |> map(fn: (r) => r._value / 1024 / 1024 / 1024, mergeKey: true)\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "last")\n  ',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b65034000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "disk")\n  |> filter(fn: (r) => r._field == "used_percent")\n  |> window(period: v.windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b65834000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "cpu")\n  |> filter(fn: (r) => r._field == "usage_user" or r._field == "usage_system" or r._field == "usage_idle")\n  |> filter(fn: (r) => r.cpu == "cpu-total")\n  |> window(period: v.windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b66034000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "system")\n  |> filter(fn: (r) => r._field == "load1" or r._field == "load5" or r._field == "load15")\n  |> window(period: v.windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b66834000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "mem")\n  |> filter(fn: (r) => r._field == "used_percent")\n  |> window(period: v.windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b67034000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "diskio")\n  |> filter(fn: (r) => r._field == "read_bytes" or r._field == "write_bytes")\n  |> derivative(unit: v.windowPeriod, nonNegative: false)\n  |> yield(name: "derivative")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b67434000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "net")\n  |> filter(fn: (r) => r._field == "bytes_recv" or r._field == "bytes_sent")\n  |> derivative(unit: v.windowPeriod, nonNegative: false)\n  |> yield(name: "derivative")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b67c34000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "processes")\n  |> filter(fn: (r) => r._field == "running" or r._field == "blocked" or r._field == "idle" or r._field == "unknown")\n  |> window(period: v.windowPeriod)\n  |> max()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "max")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
        id: '039d8c0b68434000',
        attributes: {
          properties: {
            shape: 'chronograf-v2',
            queries: [
              {
                text:
                  'from(bucket: v.bucket)\n  |> range(start: v.timeRangeStart)\n  |> filter(fn: (r) => r._measurement == "swap")\n  |> filter(fn: (r) => r._field == "total" or r._field == "used")\n  |> window(period: v.windowPeriod)\n  |> mean()\n  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")\n  |> yield(name: "mean")',
                editMode: 'advanced',
                name: '',
                builderConfig: {
                  buckets: [],
                  tags: [{key: '_measurement', values: []}],
                  functions: [],
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
      {
        id: '0399e8fd61294000',
        type: 'variable',
        attributes: {
          name: 'bucket',
          arguments: {
            type: 'query',
            values: {
              query: 'buckets()\n  |> map(fn: (r) => r.name)\n',
              language: 'flux',
            },
          },
          selected: null,
        },
      },
    ],
  },
  labels: [],
})
