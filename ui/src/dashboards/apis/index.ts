import AJAX from 'src/utils/ajax'

import {
  linksFromDashboards,
  updateDashboardLinks,
} from 'src/dashboards/utils/dashboardSwitcherLinks'

import {AxiosResponse} from 'axios'
import {
  DashboardsResponse,
  GetDashboards,
  LoadLinksOptions,
} from 'src/types/apis/dashboards'
import {DashboardSwitcherLinks} from 'src/types/dashboards'
import {Source} from 'src/types/sources'

export const getDashboards: GetDashboards = () => {
  return AJAX<DashboardsResponse>({
    method: 'GET',
    resource: 'dashboards',
  }) as Promise<AxiosResponse<DashboardsResponse>>
}

export const loadDashboardLinks = async (
  source: Source,
  {activeDashboard, dashboardsAJAX = getDashboards}: LoadLinksOptions
): Promise<DashboardSwitcherLinks> => {
  const {
    data: {dashboards},
  } = await dashboardsAJAX()

  const links = linksFromDashboards(dashboards, source)
  const dashboardLinks = updateDashboardLinks(links, activeDashboard)

  return dashboardLinks
}

export const getDashboard = async dashboardID => {
  try {
    return await AJAX({
      method: 'GET',
      url: `/chronograf/v1/dashboards/${dashboardID}`,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const updateDashboard = dashboard => {
  return AJAX({
    method: 'PUT',
    url: dashboard.links.self,
    data: dashboard,
  })
}

export const updateDashboardCell = cell => {
  return AJAX({
    method: 'PUT',
    url: cell.links.self,
    data: cell,
  })
}

export const createDashboard = async dashboard => {
  try {
    return await AJAX({
      method: 'POST',
      resource: 'dashboards',
      data: dashboard,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const deleteDashboard = async dashboard => {
  try {
    return await AJAX({
      method: 'DELETE',
      url: dashboard.links.self,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const addDashboardCell = async (dashboard, cell) => {
  try {
    return await AJAX({
      method: 'POST',
      url: dashboard.links.cells,
      data: cell,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const deleteDashboardCell = async cell => {
  try {
    return await AJAX({
      method: 'DELETE',
      url: cell.links.self,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const editTemplateVariables = async templateVariable => {
  try {
    return await AJAX({
      method: 'PUT',
      url: templateVariable.links.self,
      data: templateVariable,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
