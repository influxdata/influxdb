// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import OnboardingSideBar from 'src/onboarding/components/OnboardingSideBar'
import {cpuTelegrafPlugin, diskTelegrafPlugin} from 'mocks/dummyData'
import {Button} from 'src/clockface'
import SideBarTab from 'src/onboarding/components/side_bar/SideBarTab'

const onClick = jest.fn(() => {})

const setup = (override = {}) => {
  const props = {
    title: 'title',
    visible: true,
    telegrafPlugins: [],
    onTabClick: onClick,
    telegrafConfigID: '',
    notify: jest.fn(),
    currentStepIndex: 0,
    onNewSourceClick: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<OnboardingSideBar {...props} />)

  return {wrapper}
}

describe('OnboardingSideBar', () => {
  describe('rendering', () => {
    it('renders! wee!', () => {
      const {wrapper} = setup({
        telegrafPlugins: [cpuTelegrafPlugin, diskTelegrafPlugin],
      })

      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })
  })

  describe('if on selection step', () => {
    it('renders the tabs but no buttons', () => {
      const {wrapper} = setup({
        currentStepIndex: 2,
        telegrafPlugins: [cpuTelegrafPlugin, diskTelegrafPlugin],
      })

      const buttons = wrapper.find(Button)
      const tabs = wrapper.find(SideBarTab)

      expect(buttons.exists()).toBe(false)
      expect(tabs.exists()).toBe(true)
    })
  })

  describe('if there is not a telegraf config id', () => {
    it('renders the streaming sources button but not download config button', () => {
      const {wrapper} = setup({currentStepIndex: 3})

      const downloadButton = wrapper.find('[data-test="download"]')
      const addNewSourceButton = wrapper.find('[data-test="new"]')

      expect(downloadButton.exists()).toBe(false)
      expect(addNewSourceButton.exists()).toBe(true)
    })
  })

  describe('if there is a telegraf config id', () => {
    it('renders the streaming sources button and the download config button', () => {
      const {wrapper} = setup({
        currentStepIndex: 3,
        telegrafConfigID: 'ajefoiaijesfk',
      })

      const downloadButton = wrapper.find('[data-test="download"]')
      const addNewSourceButton = wrapper.find('[data-test="new"]')

      expect(downloadButton.exists()).toBe(true)
      expect(addNewSourceButton.exists()).toBe(true)
    })
  })
})
