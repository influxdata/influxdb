// Utils
import {
  getView as getViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis/'

// Types
import {RemoteDataState, QueryView} from 'src/types'
import {Dispatch} from 'redux'
import {View} from 'src/types'
import {Action as TimeMachineAction} from 'src/timeMachine/actions'
import {Action as CheckAction} from 'src/alerting/actions/checks'

//Actions
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {TimeMachineIDs} from 'src/timeMachine/constants'
import {setCurrentCheck} from 'src/alerting/actions/checks'

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

export const getViewForTimeMachine = (
  dashboardID: string,
  cellID: string,
  timeMachineID: TimeMachineIDs
) => async (
  dispatch: Dispatch<Action | TimeMachineAction | CheckAction>
): Promise<void> => {
  dispatch(setView(cellID, null, RemoteDataState.Loading))
  try {
    const view = (await getViewAJAX(dashboardID, cellID)) as QueryView
    if (view.properties.type === 'check') {
      dispatch(setCurrentCheck(RemoteDataState.Done, view.properties.check))
    }
    dispatch(setView(cellID, view, RemoteDataState.Done))
    dispatch(setActiveTimeMachine(timeMachineID, {view}))
  } catch {
    dispatch(setView(cellID, null, RemoteDataState.Error))
  }
}
