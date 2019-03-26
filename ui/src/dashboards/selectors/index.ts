import {get} from 'lodash'

import {AppState, View} from 'src/types'

export const getView = (state: AppState, id: string): View => {
  return get(state, `views.views.${id}.view`)
}

export const getViewsForDashboard = (
  state: AppState,
  dashboardID: string
): View[] => {
  const dashboard = state.dashboards.find(
    dashboard => dashboard.id === dashboardID
  )

  const cellIDs = new Set(dashboard.cells.map(cell => cell.id))

  const views = Object.values(state.views.views)
    .map(d => d.view)
    .filter(view => cellIDs.has(view.cellID))

  return views
}
