import {RemoteDataState} from 'src/types'
import {Label} from 'src/types/v2'

export type Action = SetLabels

interface SetLabels {
  type: 'SET_LABELS'
  payload: {
    status: RemoteDataState
    list: Label[]
  }
}

export const setLabels = (
  status: RemoteDataState,
  list?: Label[]
): SetLabels => ({
  type: 'SET_LABELS',
  payload: {status, list},
})
