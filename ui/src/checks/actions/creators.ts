// Types
import {RemoteDataState, CheckEntities} from 'src/types'
import {NormalizedSchema} from 'normalizr'
import {setLabelOnResource} from 'src/labels/actions/creators'

export type Action =
  | ReturnType<typeof setChecks>
  | ReturnType<typeof setCheck>
  | ReturnType<typeof removeCheck>
  | ReturnType<typeof removeLabelFromCheck>
  | ReturnType<typeof setLabelOnResource>

export const SET_CHECKS = 'SET_CHECKS'
export const SET_CHECK = 'SET_CHECK'
export const REMOVE_CHECK = 'REMOVE_CHECK'
export const ADD_LABEL_TO_CHECK = 'ADD_LABEL_TO_CHECK'
export const REMOVE_LABEL_FROM_CHECK = 'REMOVE_LABEL_FROM_CHECK'

type ChecksSchema<R extends string | string[]> = NormalizedSchema<
  CheckEntities,
  R
>

export const setChecks = (
  status: RemoteDataState,
  schema?: ChecksSchema<string[]>
) =>
  ({
    type: SET_CHECKS,
    status,
    schema,
  } as const)

export const setCheck = (
  id: string,
  status: RemoteDataState,
  schema?: ChecksSchema<string>
) =>
  ({
    type: SET_CHECK,
    id,
    status,
    schema,
  } as const)

export const removeCheck = (id: string) =>
  ({
    type: REMOVE_CHECK,
    id,
  } as const)

export const removeLabelFromCheck = (checkID: string, labelID: string) =>
  ({
    type: REMOVE_LABEL_FROM_CHECK,
    checkID,
    labelID,
  } as const)
