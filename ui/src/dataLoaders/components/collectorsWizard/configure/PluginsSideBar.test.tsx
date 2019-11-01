// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import PluginsSideBar from 'src/dataLoaders/components/collectorsWizard/configure/PluginsSideBar'
import {cpuTelegrafPlugin, diskTelegrafPlugin} from 'mocks/dummyData'
import SideBarTab from 'src/dataLoaders/components/side_bar/SideBarTab'

const onClick = jest.fn(() => {})

const setup = (override = {}) => {
  const props = {
    title: 'title',
    visible: true,
    telegrafPlugins: [],
    onTabClick: onClick,
    currentStepIndex: 0,
    onNewSourceClick: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<PluginsSideBar {...props} />)

  return {wrapper}
}

describe('PluginsSideBar', () => {
  describe('rendering', () => {
    it('renders! wee!', () => {
      const {wrapper} = setup({
        telegrafPlugins: [cpuTelegrafPlugin, diskTelegrafPlugin],
      })

      expect(wrapper.exists()).toBe(true)
    })
  })

  describe('if on selection step', () => {
    it('renders the tabs', () => {
      const {wrapper} = setup({
        currentStepIndex: 2,
        telegrafPlugins: [cpuTelegrafPlugin, diskTelegrafPlugin],
      })
      const tabs = wrapper.find(SideBarTab)
      expect(tabs.exists()).toBe(true)
    })
  })
})
