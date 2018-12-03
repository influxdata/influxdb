// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import VerifyDataStep from 'src/onboarding/components/verifyStep/VerifyDataStep'
import VerifyDataSwitcher from 'src/onboarding/components/verifyStep/VerifyDataSwitcher'
import {Button} from 'src/clockface'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

// Constants
import {defaultOnboardingStepProps} from 'src/onboarding/resources'

const setup = (override = {}) => {
  const props = {
    type: DataLoaderType.Empty,
    ...defaultOnboardingStepProps,
    ...override,
  }

  const wrapper = shallow(<VerifyDataStep {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.VerifyStep.VerifyDataStep', () => {
  it('renders', () => {
    const {wrapper} = setup()
    const buttons = wrapper.find(Button)
    const switcher = wrapper.find(VerifyDataSwitcher)

    expect(wrapper.exists()).toBe(true)
    expect(buttons.length).toBe(2)
    expect(switcher.exists()).toBe(true)
  })
})
