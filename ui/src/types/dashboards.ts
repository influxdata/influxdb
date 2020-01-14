import {
  View as GenView,
  Cell as GenCell,
  Dashboard as GenDashboard,
  Axis,
  ViewProperties,
  TableViewProperties,
  DashboardQuery,
  RenamableField,
  BuilderConfig,
} from 'src/client'
import {Label} from 'src/types'

export type FieldOption = RenamableField

export interface SortOptions {
  field: string
  direction: string
}

export interface DashboardDraftQuery extends DashboardQuery {
  hidden: boolean
}

export type BuilderConfigAggregateWindow = BuilderConfig['aggregateWindow']

export interface View<T extends ViewProperties = ViewProperties>
  extends GenView {
  properties: T
  cellID?: string
  dashboardID?: string
}

export interface Cell extends GenCell {
  dashboardID: string
}

export type NewCell = Omit<Cell, 'id' | 'links' | 'dashboardID'>

export interface Dashboard extends Omit<GenDashboard, 'cells'> {
  cells: Cell[]
  labels: Label[]
}

export type Base = Axis['base']

export type ViewType = ViewProperties['type']

export type ViewShape = ViewProperties['shape']

export type Omit<K, V> = Pick<K, Exclude<keyof K, V>>

export type NewView<T extends ViewProperties = ViewProperties> = Omit<
  View<T>,
  'id' | 'links'
>

export type QueryViewProperties = Extract<
  ViewProperties,
  {queries: DashboardQuery[]}
>

export type WorkingView<T extends ViewProperties> = View<T> | NewView<T>

export type QueryView = WorkingView<QueryViewProperties>

// Conditional type that narrows QueryView to those Views satisfying an
// interface, e.g. the action payload's. It's useful when a payload has a
// specific interface we know forces it to be a certain subset of
// ViewProperties.
export type ExtractWorkingView<T> = WorkingView<Extract<QueryViewProperties, T>>

export interface DashboardSwitcherLink {
  key: string
  text: string
  to: string
}

export interface DashboardSwitcherLinks {
  active?: DashboardSwitcherLink
  links: DashboardSwitcherLink[]
}

export enum NoteEditorMode {
  Adding = 'adding',
  Editing = 'editing',
}

export type TableOptions = TableViewProperties['tableOptions']

export {
  DashboardQuery,
  BuilderAggregateFunctionType,
  BuilderTagsType,
  BuilderConfig,
  ViewProperties,
  QueryEditMode,
  XYViewProperties,
  LinePlusSingleStatProperties,
  ScatterViewProperties,
  HeatmapViewProperties,
  SingleStatViewProperties,
  HistogramViewProperties,
  GaugeViewProperties,
  TableViewProperties,
  MarkdownViewProperties,
  CheckViewProperties,
  RenamableField,
  Legend,
  DecimalPlaces,
  Axes,
  Axis,
  AxisScale,
  XYGeom,
  CreateDashboardRequest,
  Threshold,
} from 'src/client'
