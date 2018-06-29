import React from 'react'
import {shallow} from 'enzyme'

import {DisconnectedAdminInfluxDBPage} from 'src/admin/containers/AdminInfluxDBPage'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import Title from 'src/reusable_ui/components/page_layout/PageHeaderTitle'
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
