// Utils
import {
  readView as readViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis/v2/view'

// Types
import {RemoteDataState} from 'src/types'
import {View} from 'src/api'
import {Dispatch} from 'redux'

export type Action = SetViewAction

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

export const readView = (id: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  dispatch(setView(id, null, RemoteDataState.Loading))

  try {
    const view = await readViewAJAX(id)

    dispatch(setView(id, view, RemoteDataState.Done))
  } catch {
    dispatch(setView(id, null, RemoteDataState.Error))
  }
}

export const updateView = (view: View) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  dispatch(setView(view.id, null, RemoteDataState.Loading))

  try {
    const newView = await updateViewAJAX(view.id, view)

    dispatch(setView(view.id, newView, RemoteDataState.Done))
  } catch {
    dispatch(setView(view.id, null, RemoteDataState.Error))
  }
}
