// APIs
import * as api from 'src/client'

// Types
import {Cell, View, NewView} from 'src/types'

export const getView = async (
  dashboardID: string,
  cellID: string
): Promise<View> => {
  const resp = await api.getDashboardsCellsView({dashboardID, cellID})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  return {...resp.data, dashboardID, cellID}
}

export const updateView = async (
  dashboardID: string,
  cellID: string,
  view: NewView
): Promise<View> => {
  const resp = await api.patchDashboardsCellsView({
    dashboardID,
    cellID,
    data: view as View,
  })

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  const viewWithIDs: View = {...resp.data, dashboardID, cellID}

  return viewWithIDs
}

export const cloneUtilFunc = async (cells: Cell[], dashboardID: string) => {
  const pendingViews = cells.map(cell =>
    api
      .getDashboardsCellsView({
        dashboardID,
        cellID: cell.id,
      })
      .then(res => {
        return {
          ...res,
          cellID: cell.id,
        }
      })
  )
  const views = await Promise.all(pendingViews)

  if (views.length > 0 && views.some(v => v.status !== 200)) {
    throw new Error('An error occurred cloning the dashboard')
  }

  return views.map(async v => {
    const view = v.data as View
    const cell = cells.find(c => c.id === view.id)

    if (cell && dashboardID) {
      const newCell = await api.postDashboardsCell({
        dashboardID,
        data: cell,
      })

      if (newCell.status !== 201) {
        throw new Error('An error occurred cloning the dashboard')
      }

      return api.patchDashboardsCellsView({
        dashboardID,
        cellID: newCell.data.id,
        data: view,
      })
    }
  })
}
