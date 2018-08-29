import {QueryConfig, TimeRange} from 'src/types'

export interface LocalStorage {
  VERSION: VERSION
  app: App
  ranges: DashboardTimeRange[]
  dataExplorer: DataExplorer
  dataExplorerQueryConfigs: DataExplorerQueryConfigs
  timeRange: TimeRange
  script: string
}

export type VERSION = string
export type timeRange = TimeRange

export interface App {
  persisted: Persisted
}

export interface DataExplorer {
  queryIDs: string[]
}

export interface DataExplorerQueryConfigs {
  [id: string]: QueryConfig
}

export interface DashboardTimeRange {
  dashboardID: number
  defaultGroupBy: string
  format: string
  inputValue: string
  lower: string
  menuOption: string
  seconds: number
  upper: string | null
}

interface Persisted {
  autoRefresh: number
  showTemplateControlBar: boolean
}
