import {
  SeverityFormatOptions,
  SeverityColorOptions,
  SeverityLevelOptions,
} from 'src/logs/constants'
import {QueryConfig, Namespace, Source} from 'src/types'
import {FieldOption} from 'src/types/dashboards'
import {TimeSeriesValue} from 'src/types/series'
import {TimeRange} from 'src/types/logs'

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
  tableInfiniteData: {
    forward: TableData
    backward: TableData
  }
  tableTime: {
    custom?: string
    relative?: string
  }
  newRowsAdded: number
}

export interface LogConfig {
  tableColumns: LogsTableColumn[]
  severityFormat: SeverityFormat
  severityLevelColors: SeverityLevelColor[]
}

export interface SeverityLevelColor {
  level: SeverityLevelOptions
  color: SeverityColorOptions
}

export interface SeverityColor {
  hex: string
  name: SeverityColorOptions
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
  name?: string
}

export enum LiveUpdating {
  Play = 'icon play',
  Pause = 'icon pause',
}
export interface TimeRange {
  upper?: string
  lower: string
  seconds?: number
  windowOption: string
  timeOption: string
}

export interface TimeBounds {
  upper: string | null
  lower: string
}

export interface TimeWindow {
  seconds: number
  windowOption: string
}

export interface TimeMarker {
  timeOption: string
}

export interface TimeRange {
  upper?: string
  lower: string
  seconds?: number
  windowOption: string
  timeOption: string
}

export interface TimeBounds {
  upper: string | null
  lower: string
}

export interface TimeWindow {
  seconds: number
  windowOption: string
}

export interface TimeMarker {
  timeOption: string
}
