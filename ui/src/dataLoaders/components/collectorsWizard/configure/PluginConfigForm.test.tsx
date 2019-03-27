// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {Form} from '@influxdata/clockface'
import {PluginConfigForm} from 'src/dataLoaders/components/collectorsWizard/configure/PluginConfigForm'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

// Constants
import {telegrafPluginsInfo} from 'src/dataLoaders/constants/pluginConfigs'
import {telegrafPlugin} from 'mocks/dummyData'

// Types
import {TelegrafPluginInputCpu} from '@influxdata/influx'

const setup = (override = {}) => {
  const props = {
    telegrafPlugin,
    configFields:
      telegrafPluginsInfo[TelegrafPluginInputCpu.NameEnum.Cpu].fields,
    onUpdateTelegrafPluginConfig: jest.fn(),
    onAddConfigValue: jest.fn(),
    onRemoveConfigValue: jest.fn(),
    authToken: '',
    onSetConfigArrayValue: jest.fn(),
    telegrafPluginName: TelegrafPluginInputCpu.NameEnum.Cpu,
    onSetActiveTelegrafPlugin: jest.fn(),
    onClickPrevious: jest.fn(),
    onClickSkip: jest.fn(),
    onClickNext: jest.fn(),
    telegrafPlugins: [],
    currentIndex: 3,
    onSetPluginConfiguration: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<PluginConfigForm {...props} />)

  return {wrapper}
}

describe('DataLoaders.Components.CollectorsWizard.Configure.PluginConfigForm', () => {
  describe('if configFields have no keys', () => {
    it('renders text and buttons', () => {
      const {wrapper} = setup({
        telegrafPlugin,
        configFields:
          telegrafPluginsInfo[TelegrafPluginInputCpu.NameEnum.Cpu].fields,
      })
      const form = wrapper.find(Form)
      const title = wrapper.find('h3')
      const onboardingButtons = wrapper.find(OnboardingButtons)

      expect(wrapper.exists()).toBe(true)
      expect(form.exists()).toBe(true)
      expect(title.exists()).toBe(true)
      expect(onboardingButtons.exists()).toBe(true)
    })
  })

  it('has a link to documentation containing plugin name', () => {
    const {wrapper} = setup({
      telegrafPlugin,
    })

    const link = wrapper.find({'data-testid': 'docs-link'})

    expect(link.exists()).toBe(true)
    expect(link.prop('href')).toContain(telegrafPlugin.name)
  })
})
