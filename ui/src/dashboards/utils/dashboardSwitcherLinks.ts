import {Dashboard} from 'src/api'
import {DashboardSwitcherLinks} from 'src/types/v2/dashboards'

export const EMPTY_LINKS = {
  links: [],
  active: null,
}

export const linksFromDashboards = (
  dashboards: Dashboard[]
): DashboardSwitcherLinks => {
  const links = dashboards.map(d => {
    return {
      key: d.id,
      text: d.name,
      to: `/dashboards/${d.id}`,
    }
  })

  return {links, active: null}
}

export const updateDashboardLinks = (
  dashboardLinks: DashboardSwitcherLinks,
  activeDashboard: Dashboard
) => {
  const {active} = dashboardLinks

  if (!active || active.key !== activeDashboard.id) {
    return updateActiveDashboardLink(dashboardLinks, activeDashboard)
  }

  return updateActiveDashboardLinkName(dashboardLinks, activeDashboard)
}

const updateActiveDashboardLink = (
  dashboardLinks: DashboardSwitcherLinks,
  dashboard: Dashboard
) => {
  if (!dashboard) {
    return {...dashboardLinks, active: null}
  }

  const active = dashboardLinks.links.find(link => link.key === dashboard.id)

  return {...dashboardLinks, active}
}

const updateActiveDashboardLinkName = (
  dashboardLinks: DashboardSwitcherLinks,
  dashboard: Dashboard
): DashboardSwitcherLinks => {
  const {name} = dashboard
  let {active} = dashboardLinks

  const links = dashboardLinks.links.map(link => {
    if (link.key === dashboard.id) {
      active = {...link, text: name}

      return active
    }

    return link
  })

  return {links, active}
}
