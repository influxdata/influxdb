// Libraries
import AJAX from 'src/utils/ajax'

// Types
import {Dashboard} from 'src/types/v2'
import {DashboardSwitcherLinks, Cell} from 'src/types/v2/dashboards'

// Utils
import {
  linksFromDashboards,
  updateDashboardLinks,
} from 'src/dashboards/utils/dashboardSwitcherLinks'

// TODO(desa): what to do about getting dashboards from another v2 source
export const getDashboards = async (url: string): Promise<Dashboard[]> => {
  try {
    const {data} = await AJAX({
      url,
    })

    return data.dashboards
  } catch (error) {
    throw error
  }
}

export const getDashboard = async (id: string): Promise<Dashboard> => {
  try {
    const {data} = await AJAX({
      url: `/api/v2/dashboards/${id}`,
    })

    return data
  } catch (error) {
    throw error
  }
}

export const createDashboard = async (
  url: string,
  dashboard: Partial<Dashboard>
): Promise<Dashboard> => {
  try {
    const {data} = await AJAX({
      method: 'POST',
      url,
      data: dashboard,
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const deleteDashboard = async (url: string): Promise<void> => {
  try {
    return await AJAX({
      method: 'DELETE',
      url,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const updateDashboard = async (
  dashboard: Dashboard
): Promise<Dashboard> => {
  try {
    const {data} = await AJAX({
      method: 'PATCH',
      url: dashboard.links.self,
      data: dashboard,
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const loadDashboardLinks = async (
  dashboardsLink: string,
  activeDashboard: Dashboard
): Promise<DashboardSwitcherLinks> => {
  const dashboards = await getDashboards(dashboardsLink)

  const links = linksFromDashboards(dashboards)
  const dashboardLinks = updateDashboardLinks(links, activeDashboard)

  return dashboardLinks
}

export const addCell = async (url: string, cell: Cell): Promise<Cell> => {
  try {
    const {data} = await AJAX({
      method: 'POST',
      url,
      data: cell,
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const updateCells = async (
  url: string,
  cells: Cell[]
): Promise<Cell[]> => {
  try {
    const {data} = await AJAX({
      method: 'PUT',
      url,
      data: cells,
    })

    return data.cells
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const deleteCell = async (url: string): Promise<void> => {
  try {
    return await AJAX({
      method: 'DELETE',
      url,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const copyCell = async (url: string, cell: Cell): Promise<Cell> => {
  try {
    const {data} = await AJAX({
      method: 'POST',
      url,
      data: cell,
    })

    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}
