// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {PluginConfigSwitcher} from 'src/dataLoaders/components/collectorsWizard/configure/PluginConfigSwitcher'
import TelegrafPluginInstructions from 'src/dataLoaders/components/collectorsWizard/configure/TelegrafPluginInstructions'
import EmptyDataSourceState from 'src/dataLoaders/components/configureStep/EmptyDataSourceState'
import PluginConfigForm from 'src/dataLoaders/components/collectorsWizard/configure/PluginConfigForm'

// Constants
import {telegrafPlugin, token} from 'mocks/dummyData'
import {TelegrafPluginInputCpu} from '@influxdata/influx'

const setup = (override = {}) => {
  const props = {
    telegrafPlugins: [],
    substepIndex: 0,
    authToken: token,
    onUpdateTelegrafPluginConfig: jest.fn(),
    onAddConfigValue: jest.fn(),
    onRemoveConfigValue: jest.fn(),
    onSetConfigArrayValue: jest.fn(),
    telegrafPluginName: TelegrafPluginInputCpu.NameEnum.Cpu,
    onClickNext: jest.fn(),
    onClickPrevious: jest.fn(),
    onClickSkip: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<PluginConfigSwitcher {...props} />)

  return {wrapper}
}

describe('DataLoading.Components.Collectors.Configure.PluginConfigSwitcher', () => {
  describe('if no telegraf plugins', () => {
    it('renders empty data source state', () => {
      const {wrapper} = setup()
      const emptyState = wrapper.find(EmptyDataSourceState)

      expect(wrapper.exists()).toBe(true)
      expect(emptyState.exists()).toBe(true)
    })
  })

  describe('if has active telegraf plugin', () => {
    it('renders plugin config form', () => {
      const {wrapper} = setup({
        telegrafPlugins: [{...telegrafPlugin, active: true}],
      })
      const form = wrapper.find(PluginConfigForm)

      expect(wrapper.exists()).toBe(true)
      expect(form.exists()).toBe(true)
    })
  })

  describe('if has no active telegraf plugin', () => {
    it('renders telegraf instructions', () => {
      const {wrapper} = setup({
        telegrafPlugins: [{...telegrafPlugin, active: false}],
      })
      const form = wrapper.find(TelegrafPluginInstructions)

      expect(wrapper.exists()).toBe(true)
      expect(form.exists()).toBe(true)
    })
  })
})
