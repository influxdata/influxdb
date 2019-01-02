// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import VerifyDataStep from 'src/onboarding/components/verifyStep/VerifyDataStep'
import VerifyDataSwitcher from 'src/onboarding/components/verifyStep/VerifyDataSwitcher'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

// Constants
import {defaultOnboardingStepProps, cpuTelegrafPlugin} from 'mocks/dummyData'

const setup = (override = {}) => {
  const props = {
    ...defaultOnboardingStepProps,
    type: DataLoaderType.Empty,
    telegrafPlugins: [],
    stepIndex: 4,
    authToken: '',
    telegrafConfigID: '',
    onSaveTelegrafConfig: jest.fn(),
    onSetActiveTelegrafPlugin: jest.fn(),
    onSetPluginConfiguration: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<VerifyDataStep {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.VerifyStep.VerifyDataStep', () => {
  it('renders', () => {
    const {wrapper} = setup()
    const onboardingButtons = wrapper.find(OnboardingButtons)
    const switcher = wrapper.find(VerifyDataSwitcher)

    expect(wrapper.exists()).toBe(true)
    expect(onboardingButtons.prop('showSkip')).toBe(true)
    expect(switcher.exists()).toBe(true)
  })

  describe('if type is streaming', () => {
    it('renders back button with correct text', () => {
      const {wrapper} = setup({
        type: DataLoaderType.Streaming,
        telegrafPlugins: [cpuTelegrafPlugin],
      })
      const onboardingButtons = wrapper.find(OnboardingButtons)

      expect(onboardingButtons.prop('nextButtonText')).toBe(
        'Continue to Completion'
      )
      expect(onboardingButtons.prop('backButtonText')).toBe(
        'Back to Cpu Configuration'
      )
    })
  })
})
