import {QueryConfig} from 'src/types'
import {ColorString} from 'src/types/colors'
import {Template} from 'src/types'

export interface Axis {
  bounds: [string, string]
  label: string
  prefix: string
  suffix: string
  base: string
  scale: string
}

export type TimeSeriesValue = string | number | null | undefined

export interface FieldOption {
  internalName: string
  displayName: string
  visible: boolean
}

export interface TableOptions {
  verticalTimeAxis: boolean
  sortBy: FieldOption
  wrapping?: string
  fixFirstColumn: boolean
}

export interface Sort {
  field: string
  direction: string
}

export interface Axes {
  x: Axis
  y: Axis
  y2?: Axis
}

interface CellLinks {
  self: string
}

// corresponds to DashboardQuery on the backend
export interface CellQuery {
  query: string
  queryConfig: QueryConfig
  source: string
  text?: string // doesn't come from server
}

export interface Legend {
  type?: string
  orientation?: string
}

export interface DecimalPlaces {
  isEnforced: boolean
  digits: number
}

export interface Cell {
  i: string
  x: number
  y: number
  w: number
  h: number
  name: string
  queries: CellQuery[]
  type: CellType
  axes: Axes
  colors: ColorString[]
  tableOptions: TableOptions
  fieldOptions: FieldOption[]
  timeFormat: string
  decimalPlaces: DecimalPlaces
  links: CellLinks
  legend: Legend
}

export enum CellType {
  Line = 'line',
  Stacked = 'line-stacked',
  StepPlot = 'line-stepplot',
  Bar = 'bar',
  LinePlusSingleStat = 'line-plus-single-stat',
  SingleStat = 'single-stat',
  Gauge = 'gauge',
  Table = 'table',
  Alerts = 'alerts',
  News = 'news',
  Guide = 'guide',
}

interface DashboardLinks {
  self: string
  cells: string
  templates: string
}

export interface Dashboard {
  id: number
  cells: Cell[]
  templates: Template[]
  name: string
  organization: string
  links?: DashboardLinks
}

export interface DashboardName {
  id: number
  name: string
  link: string
}

interface DashboardFileMetaSection {
  chronografVersion?: string
}

export interface DashboardFile {
  meta?: DashboardFileMetaSection
  dashboard: Dashboard
}

export enum ThresholdType {
  Text = 'text',
  BG = 'background',
  Base = 'base',
}
