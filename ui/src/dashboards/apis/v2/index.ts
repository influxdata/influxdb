// Libraries
import {dashboardsAPI, cellsAPI} from 'src/utils/api'

// Types
import {Dashboard, Cell, CreateCell, Label} from 'src/api'
import {DashboardSwitcherLinks} from 'src/types/v2/dashboards'

// Utils
import {
  linksFromDashboards,
  updateDashboardLinks,
} from 'src/dashboards/utils/dashboardSwitcherLinks'

// TODO(desa): what to do about getting dashboards from another v2 source
export const getDashboards = async (): Promise<Dashboard[]> => {
  const {data} = await dashboardsAPI.dashboardsGet()

  return data.dashboards
}

export const getDashboard = async (id: string): Promise<Dashboard> => {
  const {data} = await dashboardsAPI.dashboardsDashboardIDGet(id)

  return data
}

export const createDashboard = async (
  dashboard: Partial<Dashboard>
): Promise<Dashboard> => {
  const {data} = await dashboardsAPI.dashboardsPost('', dashboard)
  return data
}

export const deleteDashboard = async (dashboard: Dashboard): Promise<void> => {
  await dashboardsAPI.dashboardsDashboardIDDelete(dashboard.id)
}

export const updateDashboard = async (
  dashboard: Dashboard
): Promise<Dashboard> => {
  const {data} = await dashboardsAPI.dashboardsDashboardIDPatch(
    dashboard.id,
    dashboard
  )

  return data
}

export const loadDashboardLinks = async (
  activeDashboard: Dashboard
): Promise<DashboardSwitcherLinks> => {
  const dashboards = await getDashboards()

  const links = linksFromDashboards(dashboards)
  const dashboardLinks = updateDashboardLinks(links, activeDashboard)

  return dashboardLinks
}

export const addCell = async (
  dashboardID: string,
  cell: CreateCell
): Promise<Cell> => {
  const {data} = await cellsAPI.dashboardsDashboardIDCellsPost(
    dashboardID,
    cell
  )
  return data
}

export const updateCells = async (
  id: string,
  cells: Cell[]
): Promise<Cell[]> => {
  const {data} = await cellsAPI.dashboardsDashboardIDCellsPut(id, cells)

  return data.cells
}

export const deleteCell = async (
  dashboardID: string,
  cell: Cell
): Promise<void> => {
  await cellsAPI.dashboardsDashboardIDCellsCellIDDelete(dashboardID, cell.id)
}

export const addDashboardLabels = async (
  dashboardID: string,
  labels: Label[]
): Promise<Label[]> => {
  const addedLabels = await Promise.all(
    labels.map(async label => {
      const {data} = await dashboardsAPI.dashboardsDashboardIDLabelsPost(
        dashboardID,
        label
      )
      return data.label
    })
  )

  return addedLabels
}

export const removeDashboardLabels = async (
  dashboardID: string,
  labels: Label[]
): Promise<void> => {
  await Promise.all(
    labels.map(async label => {
      const {data} = await dashboardsAPI.dashboardsDashboardIDLabelsNameDelete(
        dashboardID,
        label.name
      )
      return data
    })
  )
}
