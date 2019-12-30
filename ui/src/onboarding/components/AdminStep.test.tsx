// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import AdminStep from 'src/onboarding/components/AdminStep'

// Dummy Data
import {defaultOnboardingStepProps} from 'mocks/dummyData'

const setup = (override = {}) => {
  const props = {
    ...defaultOnboardingStepProps,
    onSetupAdmin: jest.fn(),
    ...override,
  }

  return shallow(<AdminStep {...props} />)
}

describe('Onboarding.Components.AdminStep', () => {
  it('renders', () => {
    const wrapper = setup()

    expect(wrapper.exists()).toBe(true)
  })
})
