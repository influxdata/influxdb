import React from 'react'
import {shallow} from 'enzyme'

import {AdminInfluxDBPage} from 'src/admin/containers/AdminInfluxDBPage'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import Title from 'src/reusable_ui/components/page_layout/PageHeaderTitle'
import {source} from 'test/resources'

describe('AdminInfluxDBPage', () => {
  it('should render the appropriate header text', () => {
    const props = {
      source,
      addUser: () => {},
      loadUsers: () => {},
      loadRoles: () => {},
      loadPermissions: () => {},
      notify: () => {},
      params: {tab: ''},
      users: [],
      roles: [],
      permissions: [],
      addRole: () => {},
      removeUser: () => {},
      removeRole: () => {},
      editUser: () => {},
      editRole: () => {},
      createUser: () => {},
      createRole: () => {},
      deleteRole: () => {},
      deleteUser: () => {},
      filterRoles: () => {},
      filterUsers: () => {},
      updateRoleUsers: () => {},
      updateRolePermissions: () => {},
      updateUserPermissions: () => {},
      updateUserRoles: () => {},
      updateUserPassword: () => {},
    }

    const wrapper = shallow(<AdminInfluxDBPage {...props} />)

    const pageTitle = wrapper
      .find(PageHeader)
      .dive()
      .find(Title)
      .dive()
      .find('h1')
      .first()
      .text()

    expect(pageTitle).toBe('InfluxDB Admin')
  })
})
