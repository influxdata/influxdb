import React from 'react'

import {shallow} from 'enzyme'

import {AllUsersPage} from 'src/admin/containers/chronograf/AllUsersPage'

import {authLinks as links} from 'test/resources'

const noop = () => {}

const setup = (override = {}) => {
  const props = {
    actionsAdmin: {
      createUserAsync: noop,
      deleteUserAsync: noop,
      loadOrganizationsAsync: noop,
      loadUsersAsync: noop,
      updateUserAsync: noop,
    },
    actionsConfig: {
      getAuthConfigAsync: noop,
      updateAuthConfigAsync: noop,
    },
    authConfig: {
      superAdminNewUsers: false,
    },
    links,
    meID: '1',
    notify: noop,
    organizations: [],
    users: [],
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
