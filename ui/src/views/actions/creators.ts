// Types
import {RemoteDataState, ViewEntities} from 'src/types'
import {NormalizedSchema} from 'normalizr'
import {CellSchema} from 'src/cells/actions/creators'

// Actions
import {setDashboard} from 'src/dashboards/actions/creators'

export type Action =
  | ReturnType<typeof resetViews>
  | ReturnType<typeof setView>
  | ReturnType<typeof setViews>
  | ReturnType<typeof setViewsAndCells>
  | ReturnType<typeof setDashboard>

export const RESET_VIEWS = 'RESET_VIEWS'
export const SET_VIEW = 'SET_VIEW'
export const SET_VIEWS = 'SET_VIEWS'
export const SET_VIEWS_AND_CELLS = 'SET_VIEWS_AND_CELLS'

type ViewSchema<R extends string | string[]> = NormalizedSchema<ViewEntities, R>

export const resetViews = () =>
  ({
    type: RESET_VIEWS,
  } as const)

export const setViews = (
  status: RemoteDataState,
  schema?: ViewSchema<string[]>
) =>
  ({
    type: SET_VIEWS,
    status,
    schema,
  } as const)

export const setView = (
  id: string,
  status: RemoteDataState,
  schema?: ViewSchema<string>
) =>
  ({
    type: SET_VIEW,
    id,
    status,
    schema,
  } as const)

export const setViewsAndCells = (
  status: RemoteDataState,
  cellsArray: CellSchema<string[]>[],
  viewsArray: ViewSchema<string[]>[]
) =>
  ({
    type: SET_VIEWS_AND_CELLS,
    cellsArray,
    viewsArray,
    status,
  } as const)
