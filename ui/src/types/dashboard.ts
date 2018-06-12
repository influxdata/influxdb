import {QueryConfig} from 'src/types'
import {ColorString} from 'src/types/colors'

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

export interface TemplateValue {
  value: string
  type: string
  selected: boolean
}

export interface TemplateQuery {
  command: string
  db: string
  database?: string
  rp?: string
  measurement: string
  tagKey: string
  fieldKey: string
  influxql: string
}

export interface Template {
  id: string
  tempVar: string
  values: TemplateValue[]
  type: string
  label: string
  query?: TemplateQuery
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

interface DashboardFileMetaSection {
  chronografVersion?: string
}

export interface DashboardFile {
  meta?: DashboardFileMetaSection
  dashboard: Dashboard
}
