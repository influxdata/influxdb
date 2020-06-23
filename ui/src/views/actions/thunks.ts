// Libraries
import {normalize} from 'normalizr'
import {get} from 'lodash'
// APIs
import {
  getView as getViewAJAX,
  updateView as updateViewAJAX,
} from 'src/dashboards/apis'

// Constants
import * as copy from 'src/shared/copy/notifications'
import {viewSchema} from 'src/schemas'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {executeQueries} from 'src/timeMachine/actions/queries'
import {setView, Action} from 'src/views/actions/creators'
import {hashCode} from 'src/queryCache/actions'
import {setQueryResults} from 'src/timeMachine/actions/queries'
import {enableVEOMode} from 'src/shared/actions/app'

// Selectors
import {getViewsForDashboard} from 'src/views/selectors'
import {getByID} from 'src/resources/selectors'

// Types
import {
  RemoteDataState,
  QueryView,
  GetState,
  View,
  ViewEntities,
  TimeMachineID,
  ResourceType,
} from 'src/types'
import {Dispatch} from 'redux'

export const getView = (dashboardID: string, cellID: string) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  dispatch(setView(cellID, RemoteDataState.Loading))
  try {
    const view = await getViewAJAX(dashboardID, cellID)

    const normView = normalize<View, ViewEntities, string>(view, viewSchema)

    dispatch(setView(cellID, RemoteDataState.Done, normView))
  } catch {
    dispatch(setView(cellID, RemoteDataState.Error))
  }
}

export const updateView = (dashboardID: string, view: View) => async (
  dispatch: Dispatch<Action>
): Promise<View> => {
  const viewID = view.cellID

  dispatch(setView(viewID, RemoteDataState.Loading))

  try {
    const newView = await updateViewAJAX(dashboardID, viewID, view)

    const normView = normalize<View, ViewEntities, string>(newView, viewSchema)

    dispatch(setView(viewID, RemoteDataState.Done, normView))

    return newView
  } catch (error) {
    console.error(error)
    dispatch(setView(viewID, RemoteDataState.Error))
  }
}

export const updateViewAndVariables = (
  dashboardID: string,
  view: View
) => async (dispatch, getState: GetState) => {
  const cellID = view.cellID

  try {
    const newView = await updateViewAJAX(dashboardID, cellID, view)

    const views = getViewsForDashboard(getState(), dashboardID)

    views.splice(
      views.findIndex(v => v.id === newView.id),
      1,
      newView
    )

    const normView = normalize<View, ViewEntities, string>(newView, viewSchema)

    dispatch(setView(cellID, RemoteDataState.Done, normView))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.cellUpdateFailed()))
    dispatch(setView(cellID, RemoteDataState.Error))
  }
}

export const getViewAndResultsForVEO = (
  dashboardID: string,
  cellID: string,
  timeMachineID: TimeMachineID
) => async (dispatch, getState: GetState): Promise<void> => {
  try {
    dispatch(enableVEOMode())
    const state = getState()
    let view = getByID<View>(state, ResourceType.Views, cellID) as QueryView

    if (!view) {
      dispatch(setView(cellID, RemoteDataState.Loading))
      view = (await getViewAJAX(dashboardID, cellID)) as QueryView
    }

    dispatch(
      setActiveTimeMachine(timeMachineID, {
        contextID: dashboardID,
        view,
      })
    )
    const queries = view.properties.queries.filter(({text}) => !!text.trim())
    if (!queries.length) {
      dispatch(setQueryResults(RemoteDataState.Done, [], null))
    }
    const queryText = queries.map(({text}) => text).join('')
    const queryID = hashCode(queryText)
    const files = get(
      state,
      ['queryCache', 'queryResultsByQueryID', queryID],
      undefined
    )
    if (files) {
      dispatch(setQueryResults(RemoteDataState.Done, files, null, null))
      return
    }
    dispatch(executeQueries())
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.getViewFailed(error.message)))
    dispatch(setView(cellID, RemoteDataState.Error))
  }
}
