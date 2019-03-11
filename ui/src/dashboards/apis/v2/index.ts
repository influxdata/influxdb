// Libraries
import _ from 'lodash'

// Utils
import {addLabelDefaults} from 'src/shared/utils/labels'
import {incrementCloneName} from 'src/utils/naming'

// Types
import {Cell, NewCell, Dashboard, View} from 'src/types/v2'
import {ILabel} from '@influxdata/influx'

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
export const getDashboards = async (): Promise<Dashboard[]> => {
  const dashboards = await client.dashboards.getAll()

  return dashboards.map(d => ({
    ...d,
    labels: d.labels.map(addLabelDefaults),
    cells: addDashboardIDToCells(d.cells, d.id),
  }))
}

export const getDashboard = async (id: string): Promise<Dashboard> => {
  const dashboard = await client.dashboards.get(id)

  return {
    ...dashboard,
    labels: dashboard.labels.map(addLabelDefaults),
    cells: addDashboardIDToCells(dashboard.cells, dashboard.id),
  }
}

export const createDashboard = async (
  props: CreateDashboardRequest
): Promise<Dashboard> => {
  const dashboard = await client.dashboards.create(props)

  return {
    ...dashboard,
    labels: dashboard.labels.map(addLabelDefaults),
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
    labels: updated.labels.map(addLabelDefaults),
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

export const addDashboardLabels = async (
  dashboardID: string,
  labels: ILabel[]
): Promise<ILabel[]> => {
  const addedLabels = await Promise.all(
    labels.map(async label => {
      return client.dashboards.addLabel(dashboardID, label.id)
    })
  )

  return addedLabels as ILabel[]
}

export const removeDashboardLabels = async (
  dashboardID: string,
  labels: ILabel[]
): Promise<void> => {
  await Promise.all(
    labels.map(label => {
      return client.dashboards.removeLabel(dashboardID, label.id)
    })
  )
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
  dashboards: Dashboard[]
) => {
  const allDashboardNames = dashboards.map(d => d.name)

  const clonedName = incrementCloneName(
    allDashboardNames,
    dashboardToClone.name
  )

  if (dashboardToClone.id) {
    return client.dashboards.clone(dashboardToClone.id, clonedName)
  }
}
