// Libraries
import _ from 'lodash'

// Utils
import {incrementCloneName} from 'src/utils/naming'

// Types
import {Cell, NewCell, Dashboard, View} from 'src/types'

import {Cell as CellAPI, CreateDashboardRequest} from '@influxdata/influx'
import {client} from 'src/utils/api'

export const addDashboardIDToCells = (
  cells: CellAPI[],
  dashboardID: string
): Cell[] => {
  return cells.map(c => {
    return {...c, dashboardID}
  })
}

// TODO(desa): what to do about getting dashboards from another v2 source
export const getDashboards = async (orgID: string): Promise<Dashboard[]> => {
  const dashboards = await client.dashboards.getAll(orgID)

  return dashboards.map(d => ({
    ...d,
    cells: addDashboardIDToCells(d.cells, d.id),
  }))
}

export const getDashboard = async (id: string): Promise<Dashboard> => {
  const dashboard = await client.dashboards.get(id)

  return {
    ...dashboard,
    cells: addDashboardIDToCells(dashboard.cells, dashboard.id),
  }
}

export const createDashboard = async (
  props: CreateDashboardRequest
): Promise<Dashboard> => {
  const dashboard = await client.dashboards.create(props)

  return {
    ...dashboard,
    cells: addDashboardIDToCells(dashboard.cells, dashboard.id),
  }
}

export const deleteDashboard = async (dashboard: Dashboard): Promise<void> => {
  await client.dashboards.delete(dashboard.id)
}

export const updateDashboard = async (
  dashboard: Dashboard
): Promise<Dashboard> => {
  const updated = await client.dashboards.update(dashboard.id, dashboard)

  return {
    ...updated,
    cells: addDashboardIDToCells(updated.cells, updated.id),
  }
}

export const addCell = async (
  dashboardID: string,
  cell: NewCell
): Promise<Cell> => {
  const result = await client.dashboards.createCell(dashboardID, cell)

  const cellWithID = {...result, dashboardID}

  return cellWithID
}

export const updateCells = async (
  id: string,
  cells: Cell[]
): Promise<Cell[]> => {
  const result = await client.dashboards.updateAllCells(id, cells)

  return addDashboardIDToCells(result, id)
}

export const deleteCell = async (
  dashboardID: string,
  cell: Cell
): Promise<void> => {
  await client.dashboards.deleteCell(dashboardID, cell.id)
}

export const getView = async (
  dashboardID: string,
  cellID: string
): Promise<View> => {
  const data = await client.dashboards.getView(dashboardID, cellID)

  const view: View = {...data, dashboardID, cellID}

  return view
}

export const updateView = async (
  dashboardID: string,
  cellID: string,
  view: Partial<View>
): Promise<View> => {
  const data = await client.dashboards.updateView(dashboardID, cellID, view)

  const viewWithIDs: View = {...data, dashboardID, cellID}

  return viewWithIDs
}

export const cloneDashboard = async (
  dashboardToClone: Dashboard,
  dashboards: Dashboard[],
  orgID: string
) => {
  const allDashboardNames = dashboards.map(d => d.name)

  const clonedName = incrementCloneName(
    allDashboardNames,
    dashboardToClone.name
  )

  const clonedDashboard = await client.dashboards.clone(
    dashboardToClone.id,
    clonedName,
    orgID
  )

  return clonedDashboard
}
