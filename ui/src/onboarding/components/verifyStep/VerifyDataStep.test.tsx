// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {VerifyDataStep} from 'src/onboarding/components/verifyStep/VerifyDataStep'
import VerifyDataSwitcher from 'src/onboarding/components/verifyStep/VerifyDataSwitcher'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

// Constants
import {defaultOnboardingStepProps, withRouterProps} from 'mocks/dummyData'
import {RemoteDataState} from 'src/types'

const setup = (override = {}) => {
  const props = {
    ...defaultOnboardingStepProps,
    ...withRouterProps,
    type: DataLoaderType.Empty,
    telegrafPlugins: [],
    stepIndex: 4,
    authToken: '',
    telegrafConfigID: '',
    onSaveTelegrafConfig: jest.fn(),
    onSetActiveTelegrafPlugin: jest.fn(),
    onSetPluginConfiguration: jest.fn(),
    lpStatus: RemoteDataState.NotStarted,
    params: {stepID: '', substepID: ''},
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
})
