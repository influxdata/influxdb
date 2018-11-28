import {Color} from 'src/types/colors'
import {Dashboard} from 'src/api'

export interface Axis {
  label: string
  prefix: string
  suffix: string
  base: string
  scale: string
  bounds: [string, string]
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

export enum InfluxLanguage {
  InfluxQL = 'influxql',
  Flux = 'flux',
}

export interface DashboardQuery {
  text: string
  type: InfluxLanguage
  sourceID: string // Which source to use when running the query; may be empty, which means “use the dynamic source”
}

export interface URLQuery {
  text: string
  type: InfluxLanguage
  url: string
}

export interface Legend {
  type?: string
  orientation?: string
}

export interface DecimalPlaces {
  isEnforced: boolean
  digits: number
}

export interface View<T extends ViewProperties = ViewProperties> {
  id: string
  name: string
  properties: T
  links: ViewLinks
}

type Omit<K, V> = Pick<K, Exclude<keyof K, V>>

export type NewView<T extends ViewProperties = ViewProperties> = Omit<
  View<T>,
  'id' | 'links'
>

export interface ViewLinks {
  self: string
}

export type DygraphViewProperties = XYView | LinePlusSingleStatView

export type QueryViewProperties =
  | XYView
  | LinePlusSingleStatView
  | SingleStatView
  | TableView
  | GaugeView

export type ViewProperties =
  | QueryViewProperties
  | MarkdownView
  | EmptyView
  | LogViewerView

export interface EmptyView {
  type: ViewShape.Empty
  shape: ViewShape.Empty
}

export enum XYViewGeom {
  Line = 'line',
  Bar = 'bar',
  Step = 'step',
  Stacked = 'stacked',
}

export interface XYView {
  type: ViewType.XY
  geom: XYViewGeom
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  axes: Axes
  colors: Color[]
  legend: Legend
  note: string
  showNoteWhenEmpty: boolean
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
  note: string
  showNoteWhenEmpty: boolean
}

export interface SingleStatView {
  type: ViewType.SingleStat
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  colors: Color[]
  prefix: string
  suffix: string
  decimalPlaces: DecimalPlaces
  note: string
  showNoteWhenEmpty: boolean
}

export interface GaugeView {
  type: ViewType.Gauge
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  colors: Color[]
  prefix: string
  suffix: string
  decimalPlaces: DecimalPlaces
  note: string
  showNoteWhenEmpty: boolean
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
  note: string
  showNoteWhenEmpty: boolean
}

export interface MarkdownView {
  type: ViewType.Markdown
  shape: ViewShape.ChronografV2
  note: string
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
  XY = 'xy',
  LinePlusSingleStat = 'line-plus-single-stat',
  SingleStat = 'single-stat',
  Gauge = 'gauge',
  Table = 'table',
  Markdown = 'markdown',
  LogViewer = 'log-viewer',
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

export type NewCell = Omit<Cell, 'id' | 'viewID' | 'links'>

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

export enum NoteEditorMode {
  Adding = 'adding',
  Editing = 'editing',
}
