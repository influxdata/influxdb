import {FromFluxResult as GFFR} from '@influxdata/vis/dist/utils/fromFlux'
import {Columns} from '@influxdata/clockface'
import {Table} from '@influxdata/vis'

export type UsageQueryStatus = 'error' | 'success' | 'empty' | 'timeout'

export interface UsageRanges {
  h24: 'Past 24 Hours'
  d7: 'Past 7 Days'
  d30: 'Past 30 Days'
}

export interface UsageGraphInfo {
  title: string
  groupColumns: string[]
  column: string
  units: string
  type: 'sparkline' | 'stat'
  isGrouped: boolean
}

export interface UsageLimitStatus {
  cardinality: {status: string}
  read: {status: string}
  write: {status: string}
}

export interface UsageHistory {
  billingStats: string
  rateLimits: string
  writeMB: string
  executionSec: string
  storageGB: string
}

export interface UsageBillingStart {
  date: string
  time: string
}

export interface UsageStatusObj {
  status: 'exceeded' | string
  name: string
}

export type FromFluxResult = GFFR

export interface EmptyFluxResult {
  columns: {}
  length: 0
}

export interface UsageWidths {
  XS: Columns
  SM: Columns
  MD: Columns
}

export type UsageTable = Table
