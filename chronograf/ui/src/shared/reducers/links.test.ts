import _ from 'lodash'

import linksReducer from 'src/shared/reducers/links'
import {linksGetCompleted, setDefaultDashboard} from 'src/shared/actions/links'

const links = {
  buckets: '/v1/buckets',
  dashboards: '/v2/dashboards',
  external: {
    statusFeed: 'https://www.influxdata.com/feed/json',
  },
  flux: {
    ast: '/v2/flux/ast',
    self: '/v2/flux',
    suggestions: '/v2/flux/suggestions',
  },
  query: '/v2/query',
  sources: '/v2/sources',
  defaultDashboard: '/v2/dashboards/029d13fda9c5b000',
  organization: '/v2/org/1',
}

describe('Shared.Reducers.linksReducer', () => {
  it('can handle LINKS_GET_COMPLETED', () => {
    const actual = linksReducer(undefined, linksGetCompleted(links))
    const expected = links
    expect(_.isEqual(actual, expected)).toBe(true)
  })

  it('can reduce SET_DEFAULT_DASHBOARD_LINK', () => {
    const defaultDashboard = '/v2/dashboards/defaultiest_dashboard'
    const actual = linksReducer(links, setDefaultDashboard(defaultDashboard))

    const expected = {...links, defaultDashboard}

    expect(actual).toEqual(expected)
  })
})
