// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {ConfigFieldHandler} from 'src/dataLoaders/components/collectorsWizard/configure/ConfigFieldHandler'

// Constants
import {telegrafPluginsInfo} from 'src/dataLoaders/constants/pluginConfigs'
import {telegrafPlugin} from 'mocks/dummyData'

// Types
import {
  TelegrafPluginInputCpu,
  TelegrafPluginInputRedis,
} from '@influxdata/influx'
import ConfigFieldSwitcher from '../../configureStep/streaming/ConfigFieldSwitcher'

const setup = (override = {}) => {
  const props = {
    telegrafPlugin,
    configFields:
      telegrafPluginsInfo[TelegrafPluginInputCpu.NameEnum.Cpu].fields,
    onUpdateTelegrafPluginConfig: jest.fn(),
    onAddConfigValue: jest.fn(),
    onRemoveConfigValue: jest.fn(),
    onSetConfigArrayValue: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<ConfigFieldHandler {...props} />)

  return {wrapper}
}

describe('DataLoaders.Components.CollectorsWizard.Configure.ConfigFieldHandler', () => {
  describe('if configFields have no keys', () => {
    it('renders no config text', () => {
      const {wrapper} = setup({
        telegrafPlugin,
        configFields:
          telegrafPluginsInfo[TelegrafPluginInputCpu.NameEnum.Cpu].fields,
      })
      const noConfig = wrapper.find({
        'data-testid': 'no-config',
      })

      expect(wrapper.exists()).toBe(true)
      expect(noConfig.exists()).toBe(true)
    })
  })

  describe('if configFields have  keys', () => {
    it('renders correct number of switchers', () => {
      const configFields =
        telegrafPluginsInfo[TelegrafPluginInputRedis.NameEnum.Redis].fields
      const {wrapper} = setup({
        telegrafPlugin,
        configFields,
      })

      const switchers = wrapper.find(ConfigFieldSwitcher)

      expect(wrapper.exists()).toBe(true)
      expect(switchers.length).toBe(Object.keys(configFields).length)
    })
  })
})
