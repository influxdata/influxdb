import {AppState, Cell} from 'src/types'

export const getCells = (
  {resources}: AppState,
  dashboardID: string
): Cell[] => {
  const dashboard = resources.dashboards.byID[dashboardID]

  if (!dashboard || !dashboard.cells) {
    return []
  }

  const cellIDs = dashboard.cells

  return cellIDs.map(id => resources.cells.byID[id])
}
