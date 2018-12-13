// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import ConfigFieldSwitcher from 'src/onboarding/components/configureStep/streaming/ConfigFieldSwitcher'
import PluginConfigForm from 'src/onboarding/components/configureStep/streaming/PluginConfigForm'
import {Form} from 'src/clockface'

// Constants
import {telegrafPluginsInfo} from 'src/onboarding/constants/pluginConfigs'
import {telegrafPlugin} from 'mocks/dummyData'

// Types
import {TelegrafPluginInputCpu, TelegrafPluginInputRedis} from 'src/api'

const setup = (override = {}) => {
  const props = {
    telegrafPlugin,
    configFields:
      telegrafPluginsInfo[TelegrafPluginInputCpu.NameEnum.Cpu].fields,
    onUpdateTelegrafPluginConfig: jest.fn(),
    onSetPluginConfiguration: jest.fn(),
    onAddConfigValue: jest.fn(),
    onRemoveConfigValue: jest.fn(),
    authToken: '',
    ...override,
  }

  const wrapper = shallow(<PluginConfigForm {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConfigureStep.Streaming.PluginConfigForm', () => {
  describe('if configFields have no keys', () => {
    it('renders text', () => {
      const {wrapper} = setup({
        telegrafPlugin,
        configFields:
          telegrafPluginsInfo[TelegrafPluginInputCpu.NameEnum.Cpu].fields,
      })
      const form = wrapper.find(Form)
      const text = wrapper.find('p')

      expect(wrapper.exists()).toBe(true)
      expect(form.exists()).toBe(true)
      expect(text.exists()).toBe(true)
    })
  })

  describe('if configFields have  keys', () => {
    it('renders switcher', () => {
      const configFields =
        telegrafPluginsInfo[TelegrafPluginInputRedis.NameEnum.Redis].fields
      const {wrapper} = setup({
        telegrafPlugin,
        configFields,
      })
      const form = wrapper.find(Form)
      const configFieldSwitchers = wrapper.find(ConfigFieldSwitcher)

      const fields = Object.keys(configFields)

      expect(wrapper.exists()).toBe(true)
      expect(form.exists()).toBe(true)
      expect(configFieldSwitchers.length).toBe(fields.length)
    })
  })
})
