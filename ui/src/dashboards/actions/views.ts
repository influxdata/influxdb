// Utils
import {
  getView as getViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis/'

// Types
import {RemoteDataState} from 'src/types'
import {Dispatch} from 'redux'
import {View} from 'src/types'

export type Action = SetViewAction | SetViewsAction

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

export const getView = (dashboardID: string, cellID: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  dispatch(setView(cellID, null, RemoteDataState.Loading))
  try {
    const view = await getViewAJAX(dashboardID, cellID)

    dispatch(setView(cellID, view, RemoteDataState.Done))
  } catch {
    dispatch(setView(cellID, null, RemoteDataState.Error))
  }
}

export const updateView = (dashboardID: string, view: View) => async (
  dispatch: Dispatch<Action>
): Promise<View> => {
  const viewID = view.cellID

  dispatch(setView(viewID, view, RemoteDataState.Loading))

  try {
    const newView = await updateViewAJAX(dashboardID, viewID, view)

    dispatch(setView(viewID, newView, RemoteDataState.Done))

    return newView
  } catch {
    dispatch(setView(viewID, null, RemoteDataState.Error))
  }
}
