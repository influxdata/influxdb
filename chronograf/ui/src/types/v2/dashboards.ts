import {Color} from 'src/types/colors'

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

export enum InfluxLanguages {
  InfluxQL = 'influxql',
  Flux = 'flux',
}

export interface DashboardQuery {
  text: string
  type: InfluxLanguages
  source: string
}

export interface Legend {
  type?: string
  orientation?: string
}

export interface DecimalPlaces {
  isEnforced: boolean
  digits: number
}

export interface MarkDownProperties {
  type: ViewType.Markdown
  text: string
}

export interface View<T extends ViewProperties = ViewProperties> {
  id: string
  name: string
  properties: T
  links?: ViewLinks
}

export interface ViewLinks {
  self: string
}

export type RefreshingViewProperties =
  | LineView
  | StepPlotView
  | StackedView
  | BarChartView
  | LinePlusSingleStatView
  | SingleStatView
  | TableView
  | GaugeView

export type ViewProperties =
  | RefreshingViewProperties
  | MarkdownView
  | EmptyView
  | LogViewerView

export interface EmptyView {
  type: ViewShape.Empty
  shape: ViewShape.Empty
}

export interface LineView {
  type: ViewType.Line
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  axes: Axes
  colors: Color[]
  legend: Legend
}

export interface StackedView {
  type: ViewType.Stacked
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  axes: Axes
  colors: Color[]
  legend: Legend
}

export interface StepPlotView {
  type: ViewType.StepPlot
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  axes: Axes
  colors: Color[]
  legend: Legend
}

export interface BarChartView {
  type: ViewType.Bar
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  axes: Axes
  colors: Color[]
  legend: Legend
}

export interface LinePlusSingleStatView {
  type: ViewType.LinePlusSingleStat
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  axes: Axes
  colors: Color[]
  legend: Legend
  prefix: string
  suffix: string
  decimalPlaces: DecimalPlaces
}

export interface SingleStatView {
  type: ViewType.SingleStat
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  colors: Color[]
  prefix: string
  suffix: string
  decimalPlaces: DecimalPlaces
}

export interface GaugeView {
  type: ViewType.Gauge
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  colors: Color[]
  prefix: string
  suffix: string
  decimalPlaces: DecimalPlaces
}

export interface TableView {
  type: ViewType.Table
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  colors: Color[]
  tableOptions: TableOptions
  fieldOptions: FieldOption[]
  decimalPlaces: DecimalPlaces
  timeFormat: string
}

export interface MarkdownView {
  type: ViewType.Markdown
  shape: ViewShape.ChronografV2
  text: string
}

export interface LogViewerView {
  type: ViewType.LogViewer
  shape: ViewShape.ChronografV2
  columns: LogViewerColumn[]
}

export interface LogViewerColumn {
  name: string
  position: number
  settings: LogViewerColumnSetting[]
}

export interface LogViewerColumnSetting {
  type: string
  value: string
  name?: string
}

export enum ViewShape {
  ChronografV2 = 'chronograf-v2',
  Empty = 'empty',
}

export enum ViewType {
  Bar = 'bar',
  Line = 'line',
  Stacked = 'stacked',
  StepPlot = 'step-plot',
  LinePlusSingleStat = 'line-plus-single-stat',
  SingleStat = 'single-stat',
  Gauge = 'gauge',
  Table = 'table',
  Markdown = 'markdown',
  LogViewer = 'log-viewer',
}

interface DashboardLinks {
  self: string
  cells: string
  copy: string
}

export interface Dashboard {
  id: string
  cells: Cell[]
  name: string
  default: boolean
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

export interface ViewParams {
  type: ViewType
}
