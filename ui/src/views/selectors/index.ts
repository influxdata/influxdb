// Types
import {
  AppState,
  View,
  ResourceType,
  Dashboard,
  RemoteDataState,
} from 'src/types'

// Selectors
import {getByID} from 'src/resources/selectors'

export const getViewsForDashboard = (
  state: AppState,
  dashboardID: string
): View[] => {
  const dashboard = getByID<Dashboard>(
    state,
    ResourceType.Dashboards,
    dashboardID
  )

  const cellIDs = new Set(dashboard.cells.map(cellID => cellID))

  const views = Object.values(state.resources.views.byID).filter(
    view => view && cellIDs.has(view.cellID)
  )

  return views
}

export const getView = (state: AppState, id: string): View => {
  return getByID<View>(state, ResourceType.Views, id)
}

export const getViewStatus = (state: AppState, id: string): RemoteDataState => {
  const view = getView(state, id)
  return (view && view.status) || RemoteDataState.Loading
}
