import _ from 'lodash'

import linksReducer from 'src/shared/reducers/links'
import {linksGetCompleted, setDefaultDashboard} from 'src/shared/actions/links'

const links = {
  auths: '/api/v2/authorizations',
  buckets: '/api/v2/buckets',
  dashboards: '/api/v2/dashboards',
  external: {
    statusFeed: 'https://www.influxdata.com/feed/json',
  },
  flux: {
    ast: '/api/v2/flux/ast',
    self: '/api/v2/flux',
    suggestions: '/api/v2/flux/suggestions',
  },
  orgs: '/api/v2/orgs',
  query: '/api/v2/query',
  setup: '/api/v2/setup',
  sources: '/api/v2/sources',
  system: {
    debug: '/debug/pprof',
    health: '/healthz',
    metrics: '/metrics',
  },
  tasks: '/api/v2/tasks',
  users: '/api/v2/users',
  write: '/api/v2/write',
  defaultDashboard: '/v2/dashboards/029d13fda9c5b000',
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
