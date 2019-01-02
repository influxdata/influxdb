// Libraries
import React from 'react'
import {mount} from 'enzyme'

// Components
import ConfigFieldSwitcher from 'src/onboarding/components/configureStep/streaming/ConfigFieldSwitcher'
import PluginConfigForm from 'src/onboarding/components/configureStep/streaming/PluginConfigForm'
import {Form} from 'src/clockface'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

// Constants
import {telegrafPluginsInfo} from 'src/onboarding/constants/pluginConfigs'
import {telegrafPlugin} from 'mocks/dummyData'

// Dummy Data
import {
  cpuTelegrafPlugin,
  diskTelegrafPlugin,
  redisTelegrafPlugin,
} from 'mocks/dummyData'

// Types
import {TelegrafPluginInputCpu, TelegrafPluginInputRedis} from 'src/api'

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
    onClickPrevious: jest.fn(),
    onClickSkip: jest.fn(),
    onClickNext: jest.fn(),
    telegrafPlugins: [],
    currentIndex: 3,
    ...override,
  }

  const wrapper = mount(<PluginConfigForm {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConfigureStep.Streaming.PluginConfigForm', () => {
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

  describe('if its the first plugin', () => {
    it('renders the back button with correct text', () => {
      const {wrapper} = setup({
        telegrafPlugins: [cpuTelegrafPlugin],
        currentIndex: 0,
      })

      const onboardingButtons = wrapper.find(OnboardingButtons)

      expect(onboardingButtons.prop('backButtonText')).toBe(
        'Back to Select Streaming Sources'
      )
    })
  })

  describe('if its the last plugin', () => {
    it('renders the next button with correct text', () => {
      const {wrapper} = setup({
        telegrafPlugins: [cpuTelegrafPlugin],
        currentIndex: 0,
      })

      const onboardingButtons = wrapper.find(OnboardingButtons)

      expect(onboardingButtons.prop('nextButtonText')).toBe(
        'Continue to Verify'
      )
    })
  })

  describe('if its the neither the last or firt plugin', () => {
    it('renders the next and back buttons with correct text', () => {
      const {wrapper} = setup({
        telegrafPlugins: [
          cpuTelegrafPlugin,
          redisTelegrafPlugin,
          diskTelegrafPlugin,
        ],
        currentIndex: 1,
      })
      const onboardingButtons = wrapper.find(OnboardingButtons)

      expect(onboardingButtons.prop('nextButtonText')).toBe('Continue to Disk')
      expect(onboardingButtons.prop('backButtonText')).toBe('Back to Cpu')
    })
  })

  describe('if skip button is clicked', () => {
    it('renders the correct skip button text for streaming sources', () => {
      const {wrapper} = setup({
        telegrafPlugins: [cpuTelegrafPlugin],
        params: {stepID: '3', substepID: 'streaming'},
      })

      const skipButton = wrapper.find('[data-test="skip"]')

      expect(skipButton.prop('text')).toBe('Skip to Verify')
    })
  })
})
