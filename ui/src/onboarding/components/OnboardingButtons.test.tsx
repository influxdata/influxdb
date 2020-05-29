// Libraries
import React from 'react'
import {render, fireEvent} from 'react-testing-library'

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

  return render(<OnboardingButtons {...props} />)
}

describe('Onboarding.Components.OnboardingButtons', () => {
  describe('rendering', () => {
    it('renders next and back buttons with the correct text', () => {
      const nextButtonText = 'Continue'
      const backButtonText = 'Previous'
      const onClickBack = jest.fn()

      const {getByTestId} = setup({
        nextButtonText,
        backButtonText,
        onClickBack,
      })

      const nextButton = getByTestId('next')
      const backButton = getByTestId('back')

      const nextButtonLabel = nextButton.childNodes[0]
      const backButtonLabel = backButton.childNodes[0]

      fireEvent.click(backButton)

      expect(nextButton).toBeDefined()
      expect(nextButtonLabel.textContent).toEqual(nextButtonText)
      expect(nextButton.getAttribute('type')).toBe(ButtonType.Submit)
      expect(backButtonLabel.textContent).toEqual(backButtonText)
      expect(backButton.getAttribute('type')).toBe(ButtonType.Button)
      expect(onClickBack).toBeCalled()
    })

    describe('if show skip', () => {
      it('renders the skip button', () => {
        const onClickSkip = jest.fn()
        const {getByTestId} = setup({
          showSkip: true,
          onClickSkip,
        })

        const skipButton = getByTestId('skip')
        fireEvent.click(skipButton)

        expect(skipButton).toBeDefined()
        expect(skipButton.getAttribute('type')).toBe(ButtonType.Button)
        expect(onClickSkip).toBeCalled()
      })
    })
  })
})
