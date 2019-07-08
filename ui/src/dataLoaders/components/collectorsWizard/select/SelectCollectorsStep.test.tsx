// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {SelectCollectorsStep} from 'src/dataLoaders/components/collectorsWizard/select/SelectCollectorsStep'
import StreamingSelector from 'src/dataLoaders/components/collectorsWizard/select/StreamingSelector'
import {ComponentStatus} from 'src/clockface'

// Types
import {DataLoaderType} from 'src/types/dataLoaders'

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
    bucket: 'b1',
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

  return shallow(<SelectCollectorsStep {...props} />)
}

describe('DataLoaders.Components.CollectorsWizard.Select.SelectCollectorsStep', () => {
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
    it('renders next button with correct status', () => {
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
