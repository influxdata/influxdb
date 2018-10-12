// Utils
import {
  readView as readViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis/v2/view'

// Types
import {RemoteDataState} from 'src/types'
import {View} from 'src/types/v2'
import {Dispatch} from 'redux'

export type Action = SetActiveViewAction | SetViewAction

interface SetActiveViewAction {
  type: 'SET_ACTIVE_VIEW_ID'
  payload: {
    activeViewID: string
  }
}

export const setActiveCell = (activeViewID: string): SetActiveViewAction => ({
  type: 'SET_ACTIVE_VIEW_ID',
  payload: {activeViewID},
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
  view: View | null,
  status: RemoteDataState
): SetViewAction => ({
  type: 'SET_VIEW',
  payload: {id, view, status},
})

export const readView = (url: string, id: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  dispatch(setView(id, null, RemoteDataState.Loading))

  try {
    const view = await readViewAJAX(url)

    dispatch(setView(id, view, RemoteDataState.Done))
  } catch {
    dispatch(setView(id, null, RemoteDataState.Error))
  }
}

export const updateView = (url: string, view: View) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  dispatch(setView(view.id, null, RemoteDataState.Loading))

  try {
    const newView = await updateViewAJAX(url, view)

    dispatch(setView(view.id, newView, RemoteDataState.Done))
  } catch {
    dispatch(setView(view.id, null, RemoteDataState.Error))
  }
}
