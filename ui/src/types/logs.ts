import {QueryConfig, TimeRange, Namespace, Source} from 'src/types'

export interface Filter {
  id: string
  key: string
  value: string
  operator: string
}

export interface TableData {
  columns: string[]
  values: string[]
}

export interface LogsState {
  currentSource: Source | null
  currentNamespaces: Namespace[]
  currentNamespace: Namespace | null
  timeRange: TimeRange
  histogramQueryConfig: QueryConfig | null
  histogramData: object[]
  tableQueryConfig: QueryConfig | null
  tableData: TableData
  searchTerm: string | null
  filters: Filter[]
  queryCount: number
}

export interface SeverityLevel {
  severity: string
  default: SeverityColor
  override?: SeverityColor
}

export interface SeverityColor {
  hex: string
  name: string
}

export type SeverityFormat = 'dot' | 'dotText' | 'text'
