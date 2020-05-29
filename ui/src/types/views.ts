import {View as GenView, Axis, ViewProperties, DashboardQuery} from 'src/client'
import {RemoteDataState} from 'src/types'

export interface View<T extends ViewProperties = ViewProperties>
  extends GenView {
  properties: T
  cellID?: string
  dashboardID?: string
  status: RemoteDataState
}
export type Base = Axis['base']

export type ViewType = ViewProperties['type']

export type ViewShape = ViewProperties['shape']

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
