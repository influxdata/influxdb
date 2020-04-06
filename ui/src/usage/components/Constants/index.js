import {Columns} from '@influxdata/clockface'

export const QUERY_RESULTS_STATUS_EMPTY = 'empty'
export const QUERY_RESULTS_STATUS_ERROR = 'error'
export const QUERY_RESULTS_STATUS_SUCCESS = 'success'
export const QUERY_RESULTS_STATUS_TIMEOUT = 'timeout'

export const GRAPH_INFO = {
  titles: ['Writes', 'Query Duration', 'Storage Usage'],
  write_mb: [
    {
      title: 'Writes (MB)',
      groupColumns: [],
      column: 'write_mb',
      units: 'MB',
      isGrouped: true,
      type: 'sparkline',
    },
  ],
  execution_sec: [
    {
      title: 'Total Query Duration (s)',
      groupColumns: [],
      column: 'execution_sec',
      units: 's',
      isGrouped: false,
      type: 'sparkline',
    },
  ],
  storage_gb: [
    {
      title: 'Storage (GB-hr)',
      groupColumns: [],
      column: 'storage_gb',
      units: 'GB',
      isGrouped: false,
      type: 'sparkline',
    },
  ],
  rate_limits: [
    {
      title: 'Limit Events',
      groupColumns: ['_field'],
      column: '_value',
      units: '',
      isGrouped: true,
      type: 'sparkline',
    },
  ],
  billing_stats: [
    {
      title: 'Writes',
      groupColumns: [],
      column: 'writes_mb',
      units: 'MB',
      isGrouped: false,
      type: 'stat',
    },
    {
      title: 'Query Duration',
      groupColumns: [],
      column: 'execution_sec',
      units: 's',
      isGrouped: false,
      type: 'stat',
    },
    {
      title: 'Storage Usage',
      groupColumns: [],
      column: 'storage_gb',
      units: 'GB-hr',
      isGrouped: false,
      type: 'stat',
    },
  ],
}

export const PANEL_CONTENTS_WIDTHS = {
  usage: {
    XS: Columns.Twelve,
    SM: Columns.Twelve,
    MD: Columns.Twelve,
  },
  rate_limits: {
    XS: Columns.Twelve,
    SM: Columns.Twelve,
    MD: Columns.Twelve,
  },
  billing_stats: {
    XS: Columns.Twelve,
    SM: Columns.Six,
    MD: Columns.Two,
  },
}
