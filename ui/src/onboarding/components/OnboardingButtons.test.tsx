// Libraries
import React from 'react'
import {mount} from 'enzyme'

// Components
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import {ButtonType} from '@influxdata/clockface'

const setup = (override = {}) => {
  const props = {
    onClickBack: jest.fn(),
    nextButtonText: 'next',
    backButtonText: 'back',
    ...override,
  }

  const wrapper = mount(<OnboardingButtons {...props} />)

  return wrapper
}

describe('Onboarding.Components.OnboardingButtons', () => {
  describe('rendering', () => {
    it.only('renders next and back buttons with the correct text', () => {
      const nextButtonText = 'Continue'
      const backButtonText = 'Previous'
      const onClickBack = jest.fn()

      const wrapper = setup({
        nextButtonText,
        backButtonText,
        onClickBack,
      })

      const nextButton = wrapper.find('[data-testid="next"]')
      const backButton = wrapper.find('[data-testid="back"]')

      const nextButtonLabel = nextButton.find('.button--label')
      const backButtonLabel = backButton.find('.button--label')

      backButton.simulate('click')

      expect(wrapper.exists()).toBe(true)
      expect(nextButtonLabel.prop('children')).toEqual(nextButtonText)
      expect(nextButton.prop('type')).toBe(ButtonType.Submit)
      expect(backButtonLabel.prop('children')).toEqual(backButtonText)
      expect(backButton.prop('type')).toBe(ButtonType.Button)
      expect(onClickBack).toBeCalled()
    })

    describe('if show skip', () => {
      it('renders the skip button', () => {
        const onClickSkip = jest.fn()
        const wrapper = setup({
          showSkip: true,
          onClickSkip,
        })

        const skipButton = wrapper.find('[data-testid="skip"]')
        skipButton.simulate('click')

        expect(skipButton.exists()).toBe(true)
        expect(skipButton.prop('type')).toBe(ButtonType.Button)
        expect(onClickSkip).toBeCalled()
      })
    })
  })
})
