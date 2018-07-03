import {SeverityFormatOptions} from 'src/logs/constants'
import {QueryConfig, TimeRange, Namespace, Source} from 'src/types'
import {FieldOption} from 'src/types/dashboards'
import {TimeSeriesValue} from 'src/types/series'

export interface Filter {
  id: string
  key: string
  value: string
  operator: string
}

export interface TableData {
  columns: string[]
  values: TimeSeriesValue[][]
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
  logConfig: LogConfig
}

export interface LogConfig {
  tableColumns: LogsTableColumn[]
  severityFormat: SeverityFormat
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

export type SeverityFormat = SeverityFormatOptions

export type LogsTableColumn = FieldOption

export interface ServerLogConfig {
  columns: ServerColumn[]
}

export interface ServerColumn {
  name: string
  position: number
  encodings: ServerEncoding[]
}

export interface ServerEncoding {
  type: string
  value: string
}
