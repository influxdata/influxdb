import {loadDashboardLinks} from 'src/dashboards/apis'
import {dashboard, source} from 'test/resources'

describe('dashboards.apis.loadDashboardLinks', () => {
  const socure = {...source, id: '897'}

  const activeDashboard = {
    ...dashboard,
    id: 9001,
    name: 'Low Dash',
  }

  const dashboards = [
    {
      ...dashboard,
      id: 123,
      name: 'Test Dashboard',
    },
    activeDashboard,
    {
      ...dashboard,
      id: 2282,
      name: 'Sample Dash',
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

  const options = {
    activeDashboard,
    dashboardsAJAX: getDashboards,
  }

  it('can load dashboard links for source', async () => {
    const actualLinks = await loadDashboardLinks(socure, options)

    const expectedLinks = {
      links: [
        {
          key: '123',
          text: 'Test Dashboard',
          to: '/sources/897/dashboards/123',
        },
        {
          key: '9001',
          text: 'Low Dash',
          to: '/sources/897/dashboards/9001',
        },
        {
          key: '2282',
          text: 'Sample Dash',
          to: '/sources/897/dashboards/2282',
        },
      ],
      active: {
        key: '9001',
        text: 'Low Dash',
        to: '/sources/897/dashboards/9001',
      },
    }

    expect(actualLinks).toEqual(expectedLinks)
  })
})
