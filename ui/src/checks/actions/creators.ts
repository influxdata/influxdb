// Types
import {Check, RemoteDataState, Label} from 'src/types'

export type Action =
  | ReturnType<typeof setAllChecks>
  | ReturnType<typeof setCheck>
  | ReturnType<typeof removeCheck>
  | ReturnType<typeof addLabelToCheck>
  | ReturnType<typeof removeLabelFromCheck>

export const SET_CHECKS = 'SET_CHECKS'
export const SET_CHECK = 'SET_CHECK'
export const REMOVE_CHECK = 'REMOVE_CHECK'
export const ADD_LABEL_TO_CHECK = 'ADD_LABEL_TO_CHECK'
export const REMOVE_LABEL_FROM_CHECK = 'REMOVE_LABEL_FROM_CHECK'

export const setAllChecks = (status: RemoteDataState, checks?: Check[]) =>
  ({
    type: SET_CHECKS,
    payload: {status, checks},
  } as const)

export const setCheck = (check: Check) =>
  ({
    type: SET_CHECK,
    payload: {check},
  } as const)

export const removeCheck = (checkID: string) =>
  ({
    type: REMOVE_CHECK,
    payload: {checkID},
  } as const)

export const addLabelToCheck = (checkID: string, label: Label) =>
  ({
    type: ADD_LABEL_TO_CHECK,
    payload: {checkID, label},
  } as const)

export const removeLabelFromCheck = (checkID: string, label: Label) =>
  ({
    type: REMOVE_LABEL_FROM_CHECK,
    payload: {checkID, label},
  } as const)
