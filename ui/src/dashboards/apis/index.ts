// APIs
import {
  getDashboardsCellsView as apiGetDashboardsCellsView,
  patchDashboardsCellsView as apiPatchDashboardsCellsView,
} from 'src/client'

// Types
import {View, NewView} from 'src/types'

export const getView = async (
  dashboardID: string,
  cellID: string
): Promise<View> => {
  const resp = await apiGetDashboardsCellsView({dashboardID, cellID})

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
  const resp = await apiPatchDashboardsCellsView({
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
