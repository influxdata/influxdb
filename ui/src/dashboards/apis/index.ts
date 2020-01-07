// Libraries
import _ from 'lodash'

// APIs
import {
  getDashboards as apiGetDashboards,
  getDashboard as apiGetDashboard,
  postDashboard as apiPostDashboard,
  deleteDashboard as apiDeleteDashboard,
  patchDashboard as apiPatchDashboard,
  postDashboardsCell as apiPostDashboardsCell,
  putDashboardsCells as apiPutDashboardsCells,
  deleteDashboardsCell as apiDeleteDashboardsCell,
  getDashboardsCellsView as apiGetDashboardsCellsView,
  patchDashboardsCellsView as apiPatchDashboardsCellsView,
} from 'src/client'

// Types
import {Cell, NewCell, Dashboard, View, CreateDashboardRequest} from 'src/types'

// Utils
import {
  addDashboardDefaults,
  addDashboardIDToCells,
} from 'src/dashboards/actions'

export const getDashboards = async (orgID: string): Promise<Dashboard[]> => {
  const resp = await apiGetDashboards({query: {orgID}})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  return resp.data.dashboards.map(d => addDashboardDefaults(d))
}

export const getDashboard = async (id: string): Promise<Dashboard> => {
  const resp = await apiGetDashboard({dashboardID: id})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  return addDashboardDefaults(resp.data)
}

export const createDashboard = async (
  props: CreateDashboardRequest
): Promise<Dashboard> => {
  const resp = await apiPostDashboard({data: props})

  if (resp.status !== 201) {
    throw new Error(resp.data.message)
  }

  return addDashboardDefaults(resp.data)
}

export const deleteDashboard = async (dashboard: Dashboard): Promise<void> => {
  const resp = await apiDeleteDashboard({dashboardID: dashboard.id})

  if (resp.status !== 204) {
    throw new Error(resp.data.message)
  }
}

export const updateDashboard = async (
  dashboard: Dashboard
): Promise<Dashboard> => {
  const resp = await apiPatchDashboard({
    dashboardID: dashboard.id,
    data: dashboard,
  })

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  return addDashboardDefaults(resp.data)
}

export const addCell = async (
  dashboardID: string,
  cell: NewCell
): Promise<Cell> => {
  const resp = await apiPostDashboardsCell({dashboardID, data: cell})

  if (resp.status !== 201) {
    throw new Error(resp.data.message)
  }

  const result = resp.data

  const cellWithID = {...result, dashboardID}

  return cellWithID
}

export const updateCells = async (
  id: string,
  cells: Cell[]
): Promise<Cell[]> => {
  const resp = await apiPutDashboardsCells({dashboardID: id, data: cells})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  const result = resp.data.cells

  return addDashboardIDToCells(result, id)
}

export const deleteCell = async (
  dashboardID: string,
  cell: Cell
): Promise<void> => {
  const resp = await apiDeleteDashboardsCell({dashboardID, cellID: cell.id})

  if (resp.status !== 204) {
    throw new Error(resp.data.message)
  }
}

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
  view: Partial<View>
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
