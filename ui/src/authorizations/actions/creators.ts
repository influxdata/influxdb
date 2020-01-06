// Types
import {RemoteDataState, AuthEntities} from 'src/types'
import {NormalizedSchema} from 'normalizr'

export const SET_AUTH = 'SET_AUTH'
export const ADD_AUTH = 'ADD_AUTH'
export const EDIT_AUTH = 'EDIT_AUTH'
export const REMOVE_AUTH = 'REMOVE_AUTH'

export type Action =
  | ReturnType<typeof setAuthorizations>
  | ReturnType<typeof addAuthorization>
  | ReturnType<typeof editAuthorization>
  | ReturnType<typeof removeAuthorization>

export const setAuthorizations = (
  status: RemoteDataState,
  schema?: NormalizedSchema<AuthEntities, string[]>
) =>
  ({
    type: SET_AUTH,
    status,
    schema,
  } as const)

export const addAuthorization = (
  schema: NormalizedSchema<AuthEntities, string>
) =>
  ({
    type: ADD_AUTH,
    schema,
  } as const)

export const editAuthorization = (
  schema: NormalizedSchema<AuthEntities, string>
) =>
  ({
    type: EDIT_AUTH,
    schema,
  } as const)

export const removeAuthorization = (id: string) =>
  ({
    type: REMOVE_AUTH,
    id,
  } as const)
