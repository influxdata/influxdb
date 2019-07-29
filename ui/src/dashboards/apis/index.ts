// Libraries
import _ from 'lodash'

// APIs
import * as api from 'src/client'

// Types
import {Cell, NewCell, Dashboard, View, CreateDashboardRequest} from 'src/types'

export const addDashboardIDToCells = (
  cells: Cell[],
  dashboardID: string
): Cell[] => {
  return cells.map(c => {
    return {...c, dashboardID}
  })
}

export const getDashboards = async (orgID: string): Promise<Dashboard[]> => {
  const resp = await api.getDashboards({query: {orgID}})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  return resp.data.dashboards.map(d => ({
    ...d,
    cells: addDashboardIDToCells(d.cells as Cell[], d.id),
  }))
}

export const getDashboard = async (id: string): Promise<Dashboard> => {
  const resp = await api.getDashboard({dashboardID: id})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  const dashboard = resp.data

  return {
    ...dashboard,
    cells: addDashboardIDToCells(dashboard.cells as Cell[], dashboard.id),
  }
}

export const createDashboard = async (
  props: CreateDashboardRequest
): Promise<Dashboard> => {
  const resp = await api.postDashboard({data: props})

  if (resp.status !== 201) {
    throw new Error(resp.data.message)
  }

  const dashboard = resp.data

  return {
    ...dashboard,
    cells: addDashboardIDToCells(dashboard.cells as Cell[], dashboard.id),
  }
}

export const deleteDashboard = async (dashboard: Dashboard): Promise<void> => {
  const resp = await api.deleteDashboard({dashboardID: dashboard.id})

  if (resp.status !== 204) {
    throw new Error(resp.data.message)
  }
}

export const updateDashboard = async (
  dashboard: Dashboard
): Promise<Dashboard> => {
  const resp = await api.patchDashboard({
    dashboardID: dashboard.id,
    data: dashboard,
  })

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  const updated = resp.data

  return {
    ...updated,
    cells: addDashboardIDToCells(updated.cells as Cell[], updated.id),
  }
}

export const addCell = async (
  dashboardID: string,
  cell: NewCell
): Promise<Cell> => {
  const resp = await api.postDashboardsCell({dashboardID, data: cell})

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
  const resp = await api.putDashboardsCells({dashboardID: id, data: cells})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  const result = resp.data.cells

  return addDashboardIDToCells(result as Cell[], id)
}

export const deleteCell = async (
  dashboardID: string,
  cell: Cell
): Promise<void> => {
  const resp = await api.deleteDashboardsCell({dashboardID, cellID: cell.id})

  if (resp.status !== 204) {
    throw new Error(resp.data.message)
  }
}

export const getView = async (
  dashboardID: string,
  cellID: string
): Promise<View> => {
  const resp = await api.getDashboardsCellsView({dashboardID, cellID})

  if (resp.status !== 200) {
    throw new Error(resp.data.message)
  }

  const view: View = {...resp.data, dashboardID, cellID}

  return view
}

export const updateView = async (
  dashboardID: string,
  cellID: string,
  view: Partial<View>
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
