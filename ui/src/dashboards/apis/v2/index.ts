// Libraries
import {dashboardsAPI, cellsAPI} from 'src/utils/api'
import _ from 'lodash'

// Utils
import {addLabelDefaults} from 'src/shared/utils/labels'

// Types

import {
  DashboardSwitcherLinks,
  Cell,
  NewCell,
  Dashboard,
  View,
  Label,
} from 'src/types/v2'

import {Cell as CellTypeAPI} from 'src/api'

// Utils
import {
  linksFromDashboards,
  updateDashboardLinks,
} from 'src/dashboards/utils/dashboardSwitcherLinks'

export const addDashboardIDToCells = (
  cells: CellTypeAPI[],
  dashboardID: string
): Cell[] => {
  return cells.map(c => {
    return {...c, dashboardID}
  })
}

// TODO(desa): what to do about getting dashboards from another v2 source
export const getDashboards = async (): Promise<Dashboard[]> => {
  const {data} = await dashboardsAPI.dashboardsGet()

  return data.dashboards.map(d => ({
    ...d,
    labels: d.labels.map(addLabelDefaults),
    cells: addDashboardIDToCells(d.cells, d.id),
  }))
}

export const getDashboard = async (id: string): Promise<Dashboard> => {
  const {data} = await dashboardsAPI.dashboardsDashboardIDGet(id)

  return {
    ...data,
    labels: data.labels.map(addLabelDefaults),
    cells: addDashboardIDToCells(data.cells, data.id),
  }
}

export const createDashboard = async (
  dashboard: Partial<Dashboard>
): Promise<Dashboard> => {
  const {data} = await dashboardsAPI.dashboardsPost(dashboard)

  return {
    ...data,
    labels: data.labels.map(addLabelDefaults),
    cells: addDashboardIDToCells(data.cells, data.id),
  }
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

  return {
    ...data,
    labels: data.labels.map(addLabelDefaults),
    cells: addDashboardIDToCells(data.cells, data.id),
  }
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
  cell: NewCell
): Promise<Cell> => {
  const {data} = await cellsAPI.dashboardsDashboardIDCellsPost(
    dashboardID,
    cell
  )

  const cellWithID = {...data, dashboardID}

  return cellWithID
}

export const updateCells = async (
  id: string,
  cells: Cell[]
): Promise<Cell[]> => {
  const {data} = await cellsAPI.dashboardsDashboardIDCellsPut(id, cells)

  return addDashboardIDToCells(data.cells, id)
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
      await dashboardsAPI.dashboardsDashboardIDLabelsPost(dashboardID, {
        labelID: label.id,
      })

      return label
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
      const {
        data,
      } = await dashboardsAPI.dashboardsDashboardIDLabelsLabelIDDelete(
        dashboardID,
        label.id
      )
      return data
    })
  )
}

export const readView = async (
  dashboardID: string,
  cellID: string
): Promise<View> => {
  const {data} = await dashboardsAPI.dashboardsDashboardIDCellsCellIDViewGet(
    dashboardID,
    cellID
  )

  const view: View = {...data, dashboardID, cellID}

  return view
}

export const updateView = async (
  dashboardID: string,
  cellID: string,
  view: Partial<View>
): Promise<View> => {
  const {data} = await dashboardsAPI.dashboardsDashboardIDCellsCellIDViewPatch(
    dashboardID,
    cellID,
    view
  )

  const viewWithIDs: View = {...data, dashboardID, cellID}

  return viewWithIDs
}

export const addCellUpdateView = async (
  dashboard: Dashboard,
  cell: Cell,
  view: View
): Promise<View> => {
  const createdCell = await addCell(dashboard.id, cell)
  return updateView(dashboard.id, createdCell.id, view)
}

export const cloneDashboard = async (dashboardToClone: Dashboard) => {
  const dashboardNoCells = _.omit(dashboardToClone, ['cells'])

  const createdDashboard = await createDashboard(dashboardNoCells)

  const cells = dashboardToClone.cells

  const pendingViews = cells.map(c => readView(dashboardToClone.id, c.id))

  const views = await Promise.all(pendingViews)

  const pendingUpdatedViews = views.map(view => {
    const cell = cells.find(c => c.id === view.id)

    return addCellUpdateView(createdDashboard, cell, view)
  })

  await Promise.all(pendingUpdatedViews)

  return createdDashboard
}
