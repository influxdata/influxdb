import {getDashboards} from 'src/dashboards/apis'

import {GetDashboards} from 'src/types/apis/dashboards'
import {Source} from 'src/types/sources'
import {Dashboard, DashboardSwitcherLinks} from 'src/types/dashboards'

export const EMPTY_LINKS = {
  links: [],
  active: null,
}

export const loadDashboardLinks = async (
  source: Source,
  dashboardsAJAX: GetDashboards = getDashboards
): Promise<DashboardSwitcherLinks> => {
  const {
    data: {dashboards},
  } = await dashboardsAJAX()

  return linksFromDashboards(dashboards, source)
}

const linksFromDashboards = (
  dashboards: Dashboard[],
  source: Source
): DashboardSwitcherLinks => {
  const links = dashboards.map(d => {
    return {
      key: String(d.id),
      text: d.name,
      to: `/sources/${source.id}/dashboards/${d.id}`,
    }
  })

  return {links, active: null}
}

export const updateActiveDashboardLink = (
  dashboardLinks: DashboardSwitcherLinks,
  dashboard: Dashboard
) => {
  if (!dashboard) {
    return {...dashboardLinks, active: null}
  }

  const active = dashboardLinks.links.find(
    link => link.key === String(dashboard.id)
  )

  return {...dashboardLinks, active}
}
