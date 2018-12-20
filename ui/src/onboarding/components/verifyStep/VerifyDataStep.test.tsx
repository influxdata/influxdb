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
    expect(buttons.length).toBe(3)
    expect(switcher.exists()).toBe(true)
  })

  describe('if type is streaming', () => {
    it('renders back button with correct text', () => {
      const {wrapper} = setup({
        type: DataLoaderType.Streaming,
        telegrafPlugins: [cpuTelegrafPlugin],
      })
      const nextButton = wrapper.find('[data-test="next"]')
      const backButton = wrapper.find('[data-test="back"]')

      expect(nextButton.prop('text')).toBe('Continue to Completion')
      expect(backButton.prop('text')).toBe('Back to Cpu Configuration')
    })

    describe('when the back button is clicked', () => {
      describe('if the type is streaming', () => {
        it('calls the prop functions as expected', () => {
          const onSetSubstepIndex = jest.fn()
          const onSetActiveTelegrafPlugin = jest.fn()
          const {wrapper} = setup({
            type: DataLoaderType.Streaming,
            telegrafPlugins: [cpuTelegrafPlugin],
            onSetSubstepIndex,
            onSetActiveTelegrafPlugin,
          })
          const backButton = wrapper.find('[data-test="back"]')
          backButton.simulate('click')

          expect(onSetSubstepIndex).toBeCalledWith(3, 0)
          expect(onSetActiveTelegrafPlugin).toBeCalledWith('cpu')
        })
      })

      describe('if the type is line protocol', () => {
        it('calls the prop functions as expected', () => {
          const onDecrementCurrentStepIndex = jest.fn()
          const onSetActiveTelegrafPlugin = jest.fn()
          const {wrapper} = setup({
            type: DataLoaderType.LineProtocol,
            onDecrementCurrentStepIndex,
            onSetActiveTelegrafPlugin,
          })
          const backButton = wrapper.find('[data-test="back"]')
          backButton.simulate('click')

          expect(onDecrementCurrentStepIndex).toBeCalled()
          expect(onSetActiveTelegrafPlugin).toBeCalledWith('')
        })
      })
    })
  })
})
