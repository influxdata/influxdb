import React from 'react'
import {shallow} from 'enzyme'

import {HostsPage} from 'src/hosts/containers/HostsPage'
import HostsTable from 'src/hosts/components/HostsTable'
import SourceIndicator from 'src/shared/components/SourceIndicator'
import AutoRefreshDropdown from 'src/shared/components/AutoRefreshDropdown'

import {source} from 'test/resources'

jest.mock('src/hosts/apis', () => require('mocks/hosts/apis'))
jest.mock('src/shared/apis/env', () => require('mocks/shared/apis/env'))

import {getCpuAndLoadForHosts} from 'src/hosts/apis'

const setup = (override = {}) => {
  const props = {
    source,
    links: {environment: ''},
    autoRefresh: 0,
    manualRefresh: 0,
    onChooseAutoRefresh: () => {},
    onManualRefresh: () => {},
    notify: () => {},
    ...override,
  }

  const wrapper = shallow(<HostsPage {...props} />)
  return {wrapper, props}
}

describe('Hosts.Containers.HostsPage', () => {
  describe('rendering', () => {
    it('renders all children components', () => {
      const {wrapper} = setup()
      const sourceIndicator = wrapper.find(SourceIndicator)
      const autoRefreshDropdown = wrapper.find(AutoRefreshDropdown)
      const hostsTable = wrapper.find(HostsTable)

      expect(sourceIndicator.exists()).toBe(true)
      expect(autoRefreshDropdown.exists()).toBe(true)
      expect(hostsTable.exists()).toBe(true)
    })

    describe('hosts', () => {
      it('renders hosts when response has hosts', done => {
        const {wrapper} = setup()

        process.nextTick(() => {
          wrapper.update()
          const hostsTable = wrapper.find(HostsTable)
          expect(hostsTable.prop('hosts').length).toBe(1)
          expect(getCpuAndLoadForHosts).toHaveBeenCalledTimes(2)
          done()
        })
      })
    })
  })
})
