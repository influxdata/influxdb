// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {SelectDataSourceStep} from 'src/dataLoaders/components/selectionStep/SelectDataSourceStep'
import StreamingSelector from 'src/dataLoaders/components/selectionStep/StreamingSelector'
import TypeSelector from 'src/dataLoaders/components/selectionStep/TypeSelector'
import {ComponentStatus} from 'src/clockface'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

// Dummy Data
import {
  defaultOnboardingStepProps,
  cpuTelegrafPlugin,
  bucket,
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
    currentStepIndex: 2,
    substep: undefined,
    location: null,
    router: null,
    routes: [],
    selectedBucket: '',
    onSetBucketInfo: jest.fn(),
    buckets: [],
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

  describe('if type is line protocol', () => {
    it('renders back and next buttons with correct status', () => {
      const wrapper = setup({
        type: DataLoaderType.LineProtocol,
        buckets: [bucket],
      })
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
          currentStepIndex: 0,
          substep: 'streaming',
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
          currentStepIndex: 0,
          substep: 'streaming',
          telegrafPlugins: [cpuTelegrafPlugin],
          buckets: [bucket],
        })
        const onboardingButtons = wrapper.find(OnboardingButtons)
        expect(onboardingButtons.prop('nextButtonStatus')).toBe(
          ComponentStatus.Default
        )
      })
    })
  })
})
