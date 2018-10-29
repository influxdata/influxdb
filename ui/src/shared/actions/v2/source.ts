import {Source} from 'src/types/v2'

export enum ActionTypes {
  SetSource = 'SET_SOURCE',
  ResetSource = 'RESET_SOURCE',
}

interface SetSource {
  type: ActionTypes.SetSource
  payload: {
    source: Source
  }
}

interface ResetSource {
  type: ActionTypes.ResetSource
}

export type Actions = SetSource | ResetSource

export const setSource = (source: Source) => ({
  type: ActionTypes.SetSource,
  payload: {
    source,
  },
})

export const resetSource = () => ({
  type: ActionTypes.ResetSource,
})
