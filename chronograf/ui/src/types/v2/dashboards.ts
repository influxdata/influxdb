import {QueryConfig} from 'src/types'
import {ColorString} from 'src/types/colors'

export interface Axis {
  label: string
  prefix: string
  suffix: string
  base: string
  scale: string
  bounds?: [string, string]
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

export interface View {
  id: string
  name: string
  properties: V1View
}

export interface V1View {
  type: ViewType
  queries: CellQuery[]
  shape: ViewShape
  axes: Axes
  colors: ColorString[]
  tableOptions: TableOptions
  fieldOptions: FieldOption[]
  timeFormat: string
  decimalPlaces: DecimalPlaces
  links: CellLinks
  legend: Legend
  isWidget?: boolean
  inView: boolean
}

export enum ViewShape {
  ChronografV1 = 'chronografV1',
  Empty = 'empty',
}

export enum ViewType {
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
}

export interface Dashboard {
  id: string
  cells: Cell[]
  name: string
  links: DashboardLinks
  meta?: {[x: string]: any}
}

export interface DashboardFile {
  meta?: DashboardFileMetaSection
  dashboard: Dashboard
}

interface DashboardFileMetaSection {
  chronografVersion?: string
}

export interface Cell {
  id: string
  viewID: string
  x: number
  y: number
  w: number
  h: number
  links: {
    self: string
    view: string
    copy: string
  }
}

export type NewCell = Pick<Cell, Exclude<keyof Cell, 'id' | 'viewID' | 'links'>>

export enum ThresholdType {
  Text = 'text',
  BG = 'background',
  Base = 'base',
}

export interface DashboardSwitcherLink {
  key: string
  text: string
  to: string
}

export interface DashboardSwitcherLinks {
  active?: DashboardSwitcherLink
  links: DashboardSwitcherLink[]
}
