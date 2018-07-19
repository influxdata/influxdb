import React from 'react'
import {shallow} from 'enzyme'

import {HostsPage} from 'src/hosts/containers/HostsPage'
import HostsTable from 'src/hosts/components/HostsTable'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import Title from 'src/reusable_ui/components/page_layout/PageHeaderTitle'

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
      const hostsTable = wrapper.find(HostsTable)

      expect(hostsTable.exists()).toBe(true)

      const pageTitle = wrapper
        .find(PageHeader)
        .dive()
        .find(Title)
        .dive()
        .find('h1')
        .first()
        .text()

      expect(pageTitle).toBe('Host List')
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
