// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import PluginConfigSwitcher from 'src/onboarding/components/configureStep/streaming/PluginConfigSwitcher'
import EmptyDataSourceState from 'src/onboarding/components/configureStep/EmptyDataSourceState'
import PluginConfigForm from 'src/onboarding/components/configureStep/streaming/PluginConfigForm'

// Constants
import {telegrafPlugin, token} from 'mocks/dummyData'
import {TelegrafPluginInputCpu} from 'src/api'

const setup = (override = {}) => {
  const props = {
    telegrafPlugins: [],
    currentIndex: 0,
    authToken: token,
    onUpdateTelegrafPluginConfig: jest.fn(),
    onSetPluginConfiguration: jest.fn(),
    onAddConfigValue: jest.fn(),
    onRemoveConfigValue: jest.fn(),
    onSetConfigArrayValue: jest.fn(),
    telegrafPluginName: TelegrafPluginInputCpu.NameEnum.Cpu,
    ...override,
  }

  const wrapper = shallow(<PluginConfigSwitcher {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConfigureStep.Streaming.PluginConfigSwitcher', () => {
  describe('if no telegraf plugins', () => {
    it('renders empty data source state', () => {
      const {wrapper} = setup()
      const emptyState = wrapper.find(EmptyDataSourceState)

      expect(wrapper.exists()).toBe(true)
      expect(emptyState.exists()).toBe(true)
    })
  })

  describe('if has telegraf plugins', () => {
    it('renders plugin config form', () => {
      const {wrapper} = setup({
        telegrafPlugins: [telegrafPlugin],
      })
      const form = wrapper.find(PluginConfigForm)

      expect(wrapper.exists()).toBe(true)
      expect(form.exists()).toBe(true)
    })
  })
})
