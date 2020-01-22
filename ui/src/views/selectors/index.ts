// Types
import {AppState, View, ResourceType, Dashboard} from 'src/types'

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
