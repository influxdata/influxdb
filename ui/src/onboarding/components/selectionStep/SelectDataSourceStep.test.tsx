// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {SelectDataSourceStep} from 'src/onboarding/components/selectionStep/SelectDataSourceStep'
import StreamingSelector from 'src/onboarding/components/selectionStep/StreamingSelector'
import TypeSelector from 'src/onboarding/components/selectionStep/TypeSelector'
import {ComponentStatus} from 'src/clockface'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

// Dummy Data
import {
  defaultOnboardingStepProps,
  telegrafPlugin,
  cpuTelegrafPlugin,
} from 'mocks/dummyData'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

const setup = (override = {}) => {
  const props = {
    ...defaultOnboardingStepProps,
    bucket: '',
    telegrafPlugins: [],
    pluginBundles: [],
    type: DataLoaderType.Empty,
    onAddPluginBundle: jest.fn(),
    onRemovePluginBundle: jest.fn(),
    onSetDataLoadersType: jest.fn(),
    onSetActiveTelegrafPlugin: jest.fn(),
    onSetStepStatus: jest.fn(),
    params: {
      stepID: '2',
      substepID: undefined,
    },
    location: null,
    router: null,
    routes: [],
    ...override,
  }

  return shallow(<SelectDataSourceStep {...props} />)
}

describe('Onboarding.Components.SelectionStep.SelectDataSourceStep', () => {
  describe('if type is empty', () => {
    it('renders type selector and buttons', async () => {
      const wrapper = setup()
      const typeSelector = wrapper.find(TypeSelector)
      const onboardingButtons = wrapper.find(OnboardingButtons)

      expect(wrapper.exists()).toBe(true)
      expect(typeSelector.exists()).toBe(true)
      expect(onboardingButtons.prop('nextButtonStatus')).toBe(
        ComponentStatus.Disabled
      )
    })
  })

  describe('skip info', () => {
    it('does not render if telegraf no plugins are selected', () => {
      const wrapper = setup()
      const onboardingButtons = wrapper.find(OnboardingButtons)

      expect(onboardingButtons.prop('showSkip')).toBe(false)
    })

    it('renders if telegraf plugins are selected', () => {
      const wrapper = setup({telegrafPlugins: [cpuTelegrafPlugin]})
      const onboardingButtons = wrapper.find(OnboardingButtons)

      expect(onboardingButtons.prop('showSkip')).toBe(true)
    })

    it('does not render if any telegraf plugins is incomplete', () => {
      const wrapper = setup({telegrafPlugins: [telegrafPlugin]})
      const onboardingButtons = wrapper.find(OnboardingButtons)

      expect(onboardingButtons.prop('showSkip')).toBe(false)
    })
  })

  describe('if type is line protocol', () => {
    it('renders back and next buttons with correct status', () => {
      const wrapper = setup({type: DataLoaderType.LineProtocol})
      const onboardingButtons = wrapper.find(OnboardingButtons)
      expect(onboardingButtons.prop('nextButtonStatus')).toBe(
        ComponentStatus.Default
      )
    })
  })

  describe('if type and substep is streaming', () => {
    describe('if there are no plugins selected', () => {
      it('renders streaming selector with buttons', () => {
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          params: {stepID: '2', substepID: 'streaming'},
        })
        const streamingSelector = wrapper.find(StreamingSelector)
        const onboardingButtons = wrapper.find(OnboardingButtons)

        expect(streamingSelector.exists()).toBe(true)
        expect(onboardingButtons.prop('nextButtonStatus')).toBe(
          ComponentStatus.Disabled
        )
      })
    })

    describe('if there are plugins selected', () => {
      it('renders back and next button with correct status', () => {
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          params: {stepID: '2', substepID: 'streaming'},
          telegrafPlugins: [cpuTelegrafPlugin],
        })
        const onboardingButtons = wrapper.find(OnboardingButtons)
        expect(onboardingButtons.prop('nextButtonStatus')).toBe(
          ComponentStatus.Default
        )
      })
    })
  })
})
