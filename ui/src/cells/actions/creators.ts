// Types
import {NormalizedSchema} from 'normalizr'
import {RemoteDataState, CellEntities} from 'src/types'
import {setLabelOnResource} from 'src/labels/actions/creators'

import {setViewsAndCells} from 'src/views/actions/creators'

export const SET_CELL = 'SET_CELL'
export const SET_CELLS = 'SET_CELLS'
export const REMOVE_CELL = 'REMOVE_CELL'

export type Action =
  | ReturnType<typeof setCell>
  | ReturnType<typeof removeCell>
  | ReturnType<typeof setCells>
  | ReturnType<typeof setViewsAndCells>
  | ReturnType<typeof setLabelOnResource>

// R is the type of the value of the "result" key in normalizr's normalization
export type CellSchema<R extends string | string[]> = NormalizedSchema<
  CellEntities,
  R
>

export const setCell = (
  id: string,
  status: RemoteDataState,
  schema?: CellSchema<string>
) => ({type: SET_CELL, id, status, schema} as const)

type RemoveCellArgs = {dashboardID: string; id: string}
export const removeCell = ({dashboardID, id}: RemoveCellArgs) =>
  ({
    type: REMOVE_CELL,
    dashboardID,
    id,
  } as const)

export const setCells = (
  dashboardID: string,
  status: RemoteDataState,
  schema?: CellSchema<string[]>
) =>
  ({
    type: SET_CELLS,
    dashboardID,
    status,
    schema,
  } as const)
