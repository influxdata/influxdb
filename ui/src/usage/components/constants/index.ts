import {Columns} from '@influxdata/clockface'
import {UsageRanges, UsageGraphInfo} from 'src/types'

export const QUERY_RESULTS_STATUS_EMPTY = 'empty'
export const QUERY_RESULTS_STATUS_ERROR = 'error'
export const QUERY_RESULTS_STATUS_SUCCESS = 'success'
export const QUERY_RESULTS_STATUS_TIMEOUT = 'timeout'

export const RANGES: UsageRanges = {
  h24: 'Past 24 Hours',
  d7: 'Past 7 Days',
  d30: 'Past 30 Days',
}

export const GRAPH_INFO = {
  titles: ['Writes', 'Query Duration', 'Storage Usage'],
  writeMB: [
    {
      title: 'Writes (MB)',
      groupColumns: [],
      column: 'write_mb',
      units: 'MB',
      isGrouped: true,
      type: 'sparkline',
    },
  ] as UsageGraphInfo[],
  executionSec: [
    {
      title: 'Total Query Duration (s)',
      groupColumns: [],
      column: 'execution_sec',
      units: 's',
      isGrouped: false,
      type: 'sparkline',
    },
  ] as UsageGraphInfo[],
  storageGB: [
    {
      title: 'Storage (GB-hr)',
      groupColumns: [],
      column: 'storage_gb',
      units: 'GB',
      isGrouped: false,
      type: 'sparkline',
    },
  ] as UsageGraphInfo[],
  rateLimits: [
    {
      title: 'Limit Events',
      groupColumns: ['_field'],
      column: '_value',
      units: '',
      isGrouped: true,
      type: 'sparkline',
    },
  ] as UsageGraphInfo[],
  billingStats: [
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
  ] as UsageGraphInfo[],
}

export const PANEL_CONTENTS_WIDTHS = {
  usage: {
    XS: Columns.Twelve,
    SM: Columns.Twelve,
    MD: Columns.Twelve,
  },
  rateLimits: {
    XS: Columns.Twelve,
    SM: Columns.Twelve,
    MD: Columns.Twelve,
  },
  billingStats: {
    XS: Columns.Twelve,
    SM: Columns.Six,
    MD: Columns.Two,
  },
}
