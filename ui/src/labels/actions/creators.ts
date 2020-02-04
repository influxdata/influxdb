// Types
import {RemoteDataState, Label} from 'src/types'
import {Action as NotifyAction} from 'src/shared/actions/notifications'

export type Action =
  | ReturnType<typeof removeLabel>
  | ReturnType<typeof editLabel>
  | ReturnType<typeof setLabel>
  | ReturnType<typeof setLabels>
  | NotifyAction

export const SET_LABELS = 'SET_LABELS'
export const SET_LABEL = 'SET_LABEL'
export const EDIT_LABEL = 'EDIT_LABEL'
export const REMOVE_LABEL = 'REMOVE_LABEL'

export const setLabels = (status: RemoteDataState, list?: Label[]) =>
  ({
    type: SET_LABELS,
    payload: {status, list},
  } as const)

export const setLabel = (label: Label) =>
  ({
    type: SET_LABEL,
    payload: {label},
  } as const)

export const editLabel = (label: Label) =>
  ({
    type: EDIT_LABEL,
    payload: {label},
  } as const)

export const removeLabel = (id: string) =>
  ({
    type: REMOVE_LABEL,
    payload: {id},
  } as const)
