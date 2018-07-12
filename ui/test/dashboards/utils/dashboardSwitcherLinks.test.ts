import {
  linksFromDashboards,
  updateActiveDashboardLink,
  updateDashboardLinkName,
} from 'src/dashboards/utils/dashboardSwitcherLinks'
import {dashboard, source} from 'test/resources'

describe('dashboards.utils.dashboardSwitcherLinks', () => {
  describe('linksFromDashboards', () => {
    const socure = {...source, id: '897'}

    const dashboards = [
      {
        ...dashboard,
        id: 123,
        name: 'Test Dashboard',
      },
    ]

    it('can build dashboard links for source', () => {
      const actualLinks = linksFromDashboards(dashboards, socure)

      const expectedLinks = {
        links: [
          {
            key: '123',
            text: 'Test Dashboard',
            to: '/sources/897/dashboards/123',
          },
        ],
        active: null,
      }

      expect(actualLinks).toEqual(expectedLinks)
    })
  })

  const link1 = {
    key: '9001',
    text: 'Low Dash',
    to: '/sources/897/dashboards/9001',
  }

  const link2 = {
    key: '2282',
    text: 'Other Dash',
    to: '/sources/897/dashboards/2282',
  }

  describe('updateActiveDashboardLink', () => {
    const activeDashboard = {
      ...dashboard,
      id: 123,
      name: 'Test Dashboard',
    }

    const activeLink = {
      key: '123',
      text: 'Test Dashboard',
      to: '/sources/897/dashboards/123',
    }

    const links = [link1, activeLink, link2]
    it('can set the active link', () => {
      const loadedLinks = {links, active: null}
      const actualLinks = updateActiveDashboardLink(
        loadedLinks,
        activeDashboard
      )
      const expectedLinks = {links, active: activeLink}

      expect(actualLinks).toEqual(expectedLinks)
    })

    it('can handle a missing dashboard', () => {
      const loadedLinks = {links, active: null}
      const actualLinks = updateActiveDashboardLink(loadedLinks, undefined)
      const expectedLinks = {links, active: null}

      expect(actualLinks).toEqual(expectedLinks)
    })
  })

  describe('updateDashboardLinkName', () => {
    const staleDashboard = {
      ...dashboard,
      id: 3000,
      name: 'Stale Dashboard Name',
    }

    const staleLink = {
      key: '3000',
      text: 'Stale Dashboard Name',
      to: '/sources/897/dashboards/3000',
    }

    const links = [link1, staleLink, link2]
    const updatedDashboard = {...staleDashboard, name: 'New Dashboard Name'}

    const dashboardLinks = {
      links,
      active: link1,
    }

    it('can update the name of a provided dashboard', () => {
      const actualDashLinks = updateDashboardLinkName(
        dashboardLinks,
        updatedDashboard
      )

      const expectedDashlinks = {
        links: [
          link1,
          {
            key: '3000',
            text: 'New Dashboard Name',
            to: '/sources/897/dashboards/3000',
          },
          link2,
        ],
        active: link1,
      }

      expect(actualDashLinks).toEqual(expectedDashlinks)
    })

    it('can update name for active link', () => {
      const linksWithStaleActive = {...dashboardLinks, active: staleLink}
      const actualLinks = updateDashboardLinkName(
        linksWithStaleActive,
        updatedDashboard
      )

      const renamedLink = {
        key: '3000',
        text: 'New Dashboard Name',
        to: '/sources/897/dashboards/3000',
      }

      const expectedDashlinks = {
        links: [link1, renamedLink, link2],
        active: renamedLink,
      }

      expect(actualLinks).toEqual(expectedDashlinks)
      expect(actualLinks.active).toBe(actualLinks.links[1])
    })
  })
})
