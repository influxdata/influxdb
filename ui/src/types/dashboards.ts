import {
  Cell as GenCell,
  Dashboard as GenDashboard,
  TableViewProperties,
  DashboardQuery,
  RenamableField,
  BuilderConfig,
} from 'src/client'
import {RemoteDataState} from 'src/types'
import {Sort} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'
import {DashboardSortKey} from 'src/shared/components/resource_sort_dropdown/generateSortItems'

export type FieldOption = RenamableField

export interface SortOptions {
  field: string
  direction: string
}

export {Sort} from '@influxdata/clockface'
export {SortTypes} from 'src/shared/utils/sort'

export interface DashboardSortParams {
  sortDirection: Sort
  sortType: SortTypes
  sortKey: DashboardSortKey
}

export interface DashboardDraftQuery extends DashboardQuery {
  hidden: boolean
}

export type BuilderConfigAggregateWindow = BuilderConfig['aggregateWindow']

export interface Cell extends GenCell {
  dashboardID: string
  status: RemoteDataState
  minH?: number
  minW?: number
}

export type NewCell = Omit<Cell, 'id' | 'links' | 'dashboardID'>

export interface Dashboard extends Omit<GenDashboard, 'cells' | 'labels'> {
  cells: string[]
  labels: string[]
  status: RemoteDataState
  sortOptions: DashboardSortParams
}

export type Omit<K, V> = Pick<K, Exclude<keyof K, V>>

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
