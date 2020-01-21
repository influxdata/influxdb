// APIs
import {
  getView as getViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {executeQueries} from 'src/timeMachine/actions/queries'
import {setView, Action} from 'src/views/actions/creators'

// Selectors
import {getTimeRangeByDashboardID} from 'src/dashboards/selectors/index'
import {getView as getViewFromState} from 'src/views/selectors'

// Types
import {RemoteDataState, QueryView, GetState, View} from 'src/types'
import {Dispatch} from 'redux'
import {TimeMachineID} from 'src/types'

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
  timeMachineID: TimeMachineID
) => async (dispatch, getState: GetState): Promise<void> => {
  try {
    const state = getState()
    let view = getViewFromState(state, cellID) as QueryView

    const timeRange = getTimeRangeByDashboardID(state, dashboardID)

    if (!view) {
      dispatch(setView(cellID, null, RemoteDataState.Loading))
      view = (await getViewAJAX(dashboardID, cellID)) as QueryView
    }

    dispatch(setActiveTimeMachine(timeMachineID, {view, timeRange}))
    dispatch(executeQueries(dashboardID))
  } catch (e) {
    dispatch(notify(copy.getViewFailed(e.message)))
    dispatch(setView(cellID, null, RemoteDataState.Error))
  }
}
