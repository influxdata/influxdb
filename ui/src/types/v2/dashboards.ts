import {HistogramPosition} from 'src/minard'
import {Color} from 'src/types/colors'
import {Label} from '@influxdata/influx'
import {
  Dashboard as DashboardAPI,
  View as ViewAPI,
  Cell as CellAPI,
} from '@influxdata/influx'

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

export interface SortOptions {
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

export enum QueryEditMode {
  Builder = 'builder',
  Advanced = 'advanced',
}

export interface BuilderConfig {
  buckets: string[]
  tags: Array<{key: string; values: string[]}>
  functions: Array<{name: string}>
}

export interface DashboardQuery {
  text: string
  type: InfluxLanguage
  editMode: QueryEditMode
  builderConfig: BuilderConfig
  sourceID: string // Which source to use when running the query; may be empty, which means “use the dynamic source”
  name?: string
}

export interface DashboardDraftQuery extends DashboardQuery {
  hidden: boolean
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

export interface View<T extends ViewProperties = ViewProperties>
  extends ViewAPI {
  properties?: T
  dashboardID?: string
  cellID?: string
}

export interface Cell extends CellAPI {
  dashboardID: string
}

export interface Dashboard extends Omit<DashboardAPI, 'cells' | 'labels'> {
  cells: Cell[]
  labels: Label[]
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

export type ViewProperties =
  | XYView
  | LinePlusSingleStatView
  | SingleStatView
  | TableView
  | GaugeView
  | MarkdownView
  | EmptyView
  | LogViewerView
  | HistogramView

export type QueryViewProperties = Extract<
  ViewProperties,
  {queries: DashboardQuery[]}
>

export type WorkingView<T extends ViewProperties> = View<T> | NewView<T>
export type QueryView = WorkingView<QueryViewProperties>

/**
 * Conditional type that narrows QueryView to those Views satisfying
 * an interface, e.g. the action payload's. It's useful when a
 * payload has a specific interface we know forces it to be a
 * certain subset of ViewProperties.
 *
 * @example
 *    type xyViewPayload = typeof someXYAction.payload
 *    const workingXYView = state.view as ExtractWorkingView<xyViewPayload>
 */
export type ExtractWorkingView<T> = WorkingView<Extract<QueryViewProperties, T>>

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

export interface HistogramView {
  type: ViewType.Histogram
  shape: ViewShape.ChronografV2
  queries: DashboardQuery[]
  xColumn: string
  fillColumns: string[]
  xDomain: [number, number]
  position: HistogramPosition
  binCount: number
  colors: Color[]
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
  Histogram = 'histogram',
}

export interface DashboardFile {
  meta?: DashboardFileMetaSection
  dashboard: Dashboard
}

interface DashboardFileMetaSection {
  chronografVersion?: string
}

export type NewCell = Omit<Cell, 'id' | 'links' | 'dashboardID'>

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
