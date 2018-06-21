import React from 'react'
import {shallow} from 'enzyme'

import {DisconnectedAdminInfluxDBPage} from 'src/admin/containers/AdminInfluxDBPage'
import PageHeader from 'src/shared/components/PageHeader'
import Title from 'src/shared/components/PageHeaderTitle'
import {source} from 'test/resources'

describe('AdminInfluxDBPage', () => {
  it('should render the approriate header text', () => {
    const props = {
      source,
      loadUsers: () => {},
      loadRoles: () => {},
      loadPermissions: () => {},
      notify: () => {},
      params: {tab: ''},
    }

    const wrapper = shallow(<DisconnectedAdminInfluxDBPage {...props} />)

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
