import {HistogramPosition} from '@influxdata/giraffe'
import {Color} from 'src/types/colors'

import {
  IDashboard as DashboardAPI,
  Cell as CellAPI,
  ViewLinks,
  DashboardQuery,
  CheckViewProperties,
} from '@influxdata/influx'

export {
  CheckView,
  CheckViewProperties,
  DashboardQuery,
  BuilderConfig,
  BuilderConfigAggregateWindow,
} from '@influxdata/influx'

export enum Scale {
  Linear = 'linear',
  Log = 'log',
}

export type Base =
  | '' // Do not format using a prefix
  | '2' // Format using a binary prefix
  | '10' // Format using a decimal/SI prefix

export interface Axis {
  label: string
  prefix: string
  suffix: string
  base: Base
  scale: Scale
  bounds: [string, string] | [null, null]
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

export enum QueryEditMode {
  Builder = 'builder',
  Advanced = 'advanced',
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

export interface View<T extends ViewProperties = ViewProperties> {
  links?: ViewLinks
  id?: string
  name?: string
  properties?: T
  dashboardID?: string
  cellID?: string
}

export interface Cell extends CellAPI {
  dashboardID: string
}

export interface Dashboard extends Omit<DashboardAPI, 'cells'> {
  cells: Cell[]
}

export type Omit<K, V> = Pick<K, Exclude<keyof K, V>>

export type NewView<T extends ViewProperties = ViewProperties> = Omit<
  View<T>,
  'id' | 'links'
>

export type ViewProperties =
  | XYView
  | LinePlusSingleStatView
  | SingleStatView
  | TableView
  | GaugeView
  | MarkdownView
  | EmptyView
  | HistogramView
  | HeatmapView
  | ScatterView
  | CheckViewProperties

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
  Step = 'step',
  Bar = 'bar',
  Stacked = 'stacked',
  MonotoneX = 'monotoneX',
}

export interface XYView {
  type: ViewType.XY
  geom: XYViewGeom
  queries: DashboardQuery[]
  shape: ViewShape.ChronografV2
  axes: Axes
  xColumn?: string
  yColumn?: string
  shadeBelow?: boolean
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
  xColumn?: string
  yColumn?: string
  shadeBelow?: boolean
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
  xAxisLabel: string
  position: HistogramPosition
  binCount: number
  colors: Color[]
  note: string
  showNoteWhenEmpty: boolean
}

export interface HeatmapView {
  type: ViewType.Heatmap
  shape: ViewShape.ChronografV2
  queries: DashboardQuery[]
  xColumn: string
  yColumn: string
  xDomain: [number, number]
  yDomain: [number, number]
  xAxisLabel: string
  yAxisLabel: string
  xPrefix: string
  xSuffix: string
  yPrefix: string
  ySuffix: string
  colors: string[]
  binSize: number
  note: string
  showNoteWhenEmpty: boolean
}

export interface ScatterView {
  type: ViewType.Scatter
  shape: ViewShape.ChronografV2
  queries: DashboardQuery[]
  xColumn: string
  yColumn: string
  fillColumns: string[]
  symbolColumns: string[]
  xDomain: [number, number]
  yDomain: [number, number]
  xAxisLabel: string
  yAxisLabel: string
  xPrefix: string
  xSuffix: string
  yPrefix: string
  ySuffix: string
  colors: string[]
  note: string
  showNoteWhenEmpty: boolean
}

export interface MarkdownView {
  type: ViewType.Markdown
  shape: ViewShape.ChronografV2
  note: string
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
  Heatmap = 'heatmap',
  Scatter = 'scatter',
  Check = 'check',
}

export interface DashboardFile {
  meta?: DashboardFileMetaSection
  dashboard: Dashboard
}

interface DashboardFileMetaSection {
  chronografVersion?: string
}

export type NewCell = Omit<Cell, 'id' | 'links' | 'dashboardID'>

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
