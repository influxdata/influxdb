// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import CompletionStep from 'src/onboarding/components/CompletionStep'

// Dummy Data
import {defaultOnboardingStepProps} from 'mocks/dummyData'

const setup = (override = {}) => {
  const props = {
    ...defaultOnboardingStepProps,
    orgID: '',
    bucketID: '',
    ...override,
  }

  return shallow(<CompletionStep {...props} />)
}

describe('Onboarding.Components.CompletionStep', () => {
  it('renders', () => {
    const wrapper = setup()

    expect(wrapper.exists()).toBe(true)
  })
})
