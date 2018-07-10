import {Source} from 'src/types/sources'
import {HostNames, HostName} from 'src/types/hosts'
import {DashboardSwitcherLinks} from 'src/types/dashboards'

export const EMPTY_LINKS = {
  links: [],
  active: null,
}

export const linksFromHosts = (
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
  hostLinks: DashboardSwitcherLinks,
  host: HostName
): DashboardSwitcherLinks => {
  const active = hostLinks.links.find(link => link.key === host.name)

  return {...hostLinks, active}
}
