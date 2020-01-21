// Types
import {RemoteDataState, View} from 'src/types'

export type Action = SetViewAction | SetViewsAction | ResetViewsAction

export interface SetViewsAction {
  type: 'SET_VIEWS'
  payload: {
    views?: View[]
    status: RemoteDataState
  }
}

export const setViews = (
  status: RemoteDataState,
  views: View[]
): SetViewsAction => ({
  type: 'SET_VIEWS',
  payload: {views, status},
})

export interface SetViewAction {
  type: 'SET_VIEW'
  payload: {
    id: string
    view: View
    status: RemoteDataState
  }
}

export const setView = (
  id: string,
  view: View,
  status: RemoteDataState
): SetViewAction => ({
  type: 'SET_VIEW',
  payload: {id, view, status},
})

export interface ResetViewsAction {
  type: 'RESET_VIEWS'
}

export const resetViews = (): ResetViewsAction => ({
  type: 'RESET_VIEWS',
})
