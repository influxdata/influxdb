import React from 'react'
import {shallow} from 'enzyme'

import {HostsPage} from 'src/hosts/containers/HostsPage'

jest.mock('src/hosts/apis', () => require('mocks/hosts/apis'))
jest.mock('src/shared/apis/env', () => require('mocks/shared/apis/env'))

// const getCpuAndLoadForHosts = jest.fn()
import {getCpuAndLoadForHosts} from 'src/hosts/apis'
const props = {
  source: {
    id: '',
    name: '',
    type: '',
    links: {proxy: ''},
    telegraf: '',
  },
  links: {environment: ''},
  autoRefresh: 0,
  manualRefresh: 0,
  onChooseAutoRefresh: () => {},
  onManualRefresh: () => {},
  notify: () => {},
}

describe('hosts.containers.HostsPage', () => {
  describe('ajax requests', () => {
    it('fetches host data from proxy', async () => {
      const wrapper = shallow(<HostsPage {...props} />)
      const instance: HostsPage = wrapper.instance()
      const spy = await jest.spyOn(instance, 'fetchHostsData')

      expect(spy).toHaveBeenCalled()
      // expect(getCpuAndLoadForHosts).toHaveBeenCalled()
    })
  })
})
