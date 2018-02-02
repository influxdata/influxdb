import _ from 'lodash'

import linksReducer from 'shared/reducers/links'

import {linksGetCompleted} from 'shared/actions/links'
import {noop} from 'shared/actions/app'

const links = {
  layouts: '/chronograf/v1/layouts',
  mappings: '/chronograf/v1/mappings',
  sources: '/chronograf/v1/sources',
  me: '/chronograf/v1/me',
  dashboards: '/chronograf/v1/dashboards',
  auth: [
    {
      name: 'github',
      label: 'Github',
      login: '/oauth/github/login',
      logout: '/oauth/github/logout',
      callback: '/oauth/github/callback',
    },
  ],
  logout: '/oauth/logout',
  external: {statusFeed: 'http://pineapple.life'},
}

describe('Shared.Reducers.linksReducer', () => {
  it('can handle LINKS_GET_COMPLETED', () => {
    const initial = linksReducer(undefined, noop())
    const actual = linksReducer(initial, linksGetCompleted(links))
    const expected = links
    expect(_.isEqual(actual, expected)).to.equal(true)
  })
})
