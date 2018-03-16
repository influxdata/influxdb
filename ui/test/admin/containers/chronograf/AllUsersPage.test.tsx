import React from 'react'
import {AllUsersPage} from 'src/admin/containers/chronograf/AllUsersPage'
import {shallow} from 'enzyme'

import {authLinks as links} from 'test/resources'

const noop = () => {}

const setup = (override = {}) => {
  const props = {
    links,
    meID: '1',
    users: [],
    organizations: [],
    actionsAdmin: {
      loadUsersAsync: noop,
      loadOrganizationsAsync: noop,
      createUserAsync: noop,
      updateUserAsync: noop,
      deleteUserAsync: noop,
    },
    actionsConfig: {
      getAuthConfigAsync: noop,
      updateAuthConfigAsync: noop,
    },
    authConfig: {
      superAdminNewUsers: false,
    },
    notify: noop,
    ...override,
  }

  const wrapper = shallow(<AllUsersPage {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Admin.Containers.Chronograf.AllUsersPage', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })
  })
})
