// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {LineProtocol} from 'src/onboarding/components/configureStep/lineProtocol/LineProtocol'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

const setup = (override?) => {
  const props = {
    bucket: 'a',
    org: 'a',
    ...override,
  }
  const wrapper = shallow(<LineProtocol {...props} />)

  return {wrapper}
}

describe('LineProtocol', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })
  })

  describe('if type is not streaming', () => {
    it('renders back, next, and skip buttons with correct text', () => {
      const {wrapper} = setup()
      const buttons = wrapper.find(OnboardingButtons)

      expect(buttons.prop('backButtonText')).toBe(
        'Back to Select Data Source Type'
      )
      expect(buttons.prop('nextButtonText')).toBe('Continue to Verify')
      expect(buttons.prop('skipButtonText')).toBe('Skip Config')
    })
  })
})
