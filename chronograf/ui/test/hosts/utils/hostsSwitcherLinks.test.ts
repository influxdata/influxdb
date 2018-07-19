import {
  updateActiveHostLink,
  linksFromHosts,
} from 'src/hosts/utils/hostsSwitcherLinks'
import {source} from 'test/resources'
import {HostNames} from 'src/types/hosts'

describe('hosts.utils.hostSwitcherLinks', () => {
  describe('linksFromHosts', () => {
    const socure = {...source, id: '897'}

    const hostNames: HostNames = {
      'zelda.local': {
        name: 'zelda.local',
      },
      'gannon.local': {
        name: 'gannon.local',
      },
    }

    it('can build host links for a given source', () => {
      const actualLinks = linksFromHosts(hostNames, socure)

      const expectedLinks = {
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
        ],
        active: null,
      }

      expect(actualLinks).toEqual(expectedLinks)
    })
  })

  describe('updateActiveHostLink', () => {
    const link1 = {
      key: 'korok.local',
      text: 'korok.local',
      to: '/sources/897/hosts/korok.local',
    }

    const link2 = {
      key: 'deku.local',
      text: 'deku.local',
      to: '/sources/897/hosts/deku.local',
    }

    const activeLink = {
      key: 'robbie.local',
      text: 'robbie.local',
      to: '/sources/897/hosts/robbie.local',
    }

    const activeHostName = {
      name: 'robbie.local',
    }

    const links = [link1, activeLink, link2]

    it('can set the active host link', () => {
      const loadedLinks = {
        links,
        active: null,
      }
      const actualLinks = updateActiveHostLink(loadedLinks, activeHostName)
      const expectedLinks = {links, active: activeLink}

      expect(actualLinks).toEqual(expectedLinks)
    })
  })
})
