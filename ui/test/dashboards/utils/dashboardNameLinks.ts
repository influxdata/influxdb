import {
  loadDashboardLinks,
  updateActiveDashboardLink,
} from 'src/dashboards/utils/dashboardSwitcherLinks'
import {dashboard, source} from 'test/resources'

describe('dashboards.utils.dashboardSwitcherLinks', () => {
  describe('loadDashboardLinks', () => {
    const socure = {...source, id: '897'}

    const dashboards = [
      {
        ...dashboard,
        id: 123,
        name: 'Test Dashboard',
      },
    ]

    const data = {
      dashboards,
    }

    const axiosResponse = {
      data,
      status: 200,
      statusText: 'Okay',
      headers: null,
      config: null,
    }

    const getDashboards = async () => axiosResponse

    it('can load dashboard links for source', async () => {
      const actualLinks = await loadDashboardLinks(socure, getDashboards)

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

    const link1 = {
      key: '9001',
      text: 'Low Dash',
      to: '/sources/897/dashboards/9001',
    }

    const link2 = {
      key: '2282',
      text: 'Low Dash',
      to: '/sources/897/dashboards/2282',
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
})
