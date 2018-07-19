import {loadHostsLinks} from 'src/hosts/apis'
import {source} from 'test/resources'

import {HostNames} from 'src/types/hosts'
import {DashboardSwitcherLinks} from 'src/types/dashboards'

describe('hosts.apis.loadHostLinks', () => {
  const socure = {...source, id: '897'}

  const hostNames: HostNames = {
    'zelda.local': {
      name: 'zelda.local',
    },
    'gannon.local': {
      name: 'gannon.local',
    },
    'korok.local': {
      name: 'korok.local',
    },
  }

  const hostNamesAJAX = async () => hostNames

  const options = {
    activeHost: {
      name: 'korok.local',
    },
    getHostNamesAJAX: hostNamesAJAX,
  }

  it('can load the host links', async () => {
    const hostLinks = await loadHostsLinks(socure, options)

    const expectedLinks: DashboardSwitcherLinks = {
      active: {
        key: 'korok.local',
        text: 'korok.local',
        to: '/sources/897/hosts/korok.local',
      },
      links: [
        {
          key: 'zelda.local',
          text: 'zelda.local',
          to: '/sources/897/hosts/zelda.local',
        },
        {
          key: 'gannon.local',
          text: 'gannon.local',
          to: '/sources/897/hosts/gannon.local',
        },
        {
          key: 'korok.local',
          text: 'korok.local',
          to: '/sources/897/hosts/korok.local',
        },
      ],
    }

    expect(hostLinks).toEqual(expectedLinks)
  })
})
