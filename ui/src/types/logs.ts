import {QueryConfig, TimeRange, Namespace, Source} from 'src/types'

export interface Filter {
  id: string
  key: string
  value: string
  operator: string
}

export interface LogsState {
  currentSource: Source | null
  currentNamespaces: Namespace[]
  currentNamespace: Namespace | null
  timeRange: TimeRange
  histogramQueryConfig: QueryConfig | null
  histogramData: object[]
  tableQueryConfig: QueryConfig | null
  tableData: object[]
  searchTerm: string | null
  filters: Filter[]
}
