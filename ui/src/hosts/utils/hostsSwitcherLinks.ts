import {getAllHosts as getAllHostsAJAX} from 'src/hosts/apis'

import {GetAllHosts} from 'src/types/apis/hosts'
import {Source} from 'src/types/sources'
import {HostNames, HostName} from 'src/types/hosts'
import {DashboardSwitcherLinks} from 'src/types/dashboards'

export const EMPTY_LINKS = {
  links: [],
  active: null,
}

const hostNamesAJAX = getAllHostsAJAX as GetAllHosts

export const loadHostsLinks = async (
  source: Source,
  getHostNamesAJAX: GetAllHosts = hostNamesAJAX
): Promise<DashboardSwitcherLinks> => {
  const hostNames = await getHostNamesAJAX(source)

  return linksFromHosts(hostNames, source)
}

const linksFromHosts = (
  hostNames: HostNames,
  source: Source
): DashboardSwitcherLinks => {
  const links = Object.values(hostNames).map(h => {
    return {
      key: h.name,
      text: h.name,
      to: `/sources/${source.id}/hosts/${h.name}`,
    }
  })

  return {links, active: null}
}

export const updateActiveHostLink = (
  dashboardLinks: DashboardSwitcherLinks,
  host: HostName
) => {
  const active = dashboardLinks.links.find(link => link.key === host.name)

  return {...dashboardLinks, active}
}
