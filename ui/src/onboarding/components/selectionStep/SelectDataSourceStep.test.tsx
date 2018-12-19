// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {SelectDataSourceStep} from 'src/onboarding/components/selectionStep/SelectDataSourceStep'
import StreamingSelector from 'src/onboarding/components/selectionStep/StreamingSelector'
import TypeSelector from 'src/onboarding/components/selectionStep/TypeSelector'
import {ComponentStatus} from 'src/clockface'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

// Dummy Data
import {
  defaultOnboardingStepProps,
  telegrafPlugin,
  cpuTelegrafPlugin,
} from 'mocks/dummyData'

const setup = (override = {}) => {
  const props = {
    ...defaultOnboardingStepProps,
    bucket: '',
    telegrafPlugins: [],
    pluginBundles: [],
    type: DataLoaderType.Empty,
    onAddPluginBundle: jest.fn(),
    onRemovePluginBundle: jest.fn(),
    onSetDataLoadersType: jest.fn(),
    onSetActiveTelegrafPlugin: jest.fn(),
    onSetStepStatus: jest.fn(),
    params: {
      stepID: '2',
      substepID: undefined,
    },
    location: null,
    router: null,
    routes: [],
    ...override,
  }

  return shallow(<SelectDataSourceStep {...props} />)
}

describe('Onboarding.Components.SelectionStep.SelectDataSourceStep', () => {
  describe('if type is empty', () => {
    it('renders type selector and buttons', async () => {
      const wrapper = setup()
      const typeSelector = wrapper.find(TypeSelector)
      const backButton = wrapper.find('[data-test="back"]')
      const nextButton = wrapper.find('[data-test="next"]')

      expect(wrapper.exists()).toBe(true)
      expect(typeSelector.exists()).toBe(true)
      expect(backButton.prop('text')).toBe('Back to Admin Setup')
      expect(nextButton.prop('text')).toBe('Continue to Configuration')
      expect(nextButton.prop('status')).toBe(ComponentStatus.Disabled)
    })

    describe('if back button is clicked', () => {
      it('calls prop functions as expected', () => {
        const onDecrementCurrentStepIndex = jest.fn()
        const wrapper = setup({
          onDecrementCurrentStepIndex,
        })
        const backButton = wrapper.find('[data-test="back"]')
        backButton.simulate('click')

        expect(onDecrementCurrentStepIndex).toBeCalled()
      })
    })

    describe('if next button is clicked', () => {
      it('calls prop functions as expected', () => {
        const onIncrementCurrentStepIndex = jest.fn()
        const wrapper = setup({onIncrementCurrentStepIndex})
        const nextButton = wrapper.find('[data-test="next"]')
        nextButton.simulate('click')

        expect(onIncrementCurrentStepIndex).toBeCalled()
      })
    })
  })

  describe('skip link', () => {
    it('does not render if telegraf no plugins are selected', () => {
      const wrapper = setup()
      const skipLink = wrapper.find('[data-test="skip"]')

      expect(skipLink.exists()).toBe(false)
    })

    it('renders if telegraf plugins are selected', () => {
      const wrapper = setup({telegrafPlugins: [cpuTelegrafPlugin]})
      const skipLink = wrapper.find('[data-test="skip"]')

      expect(skipLink.exists()).toBe(true)
    })

    it('does not render if any telegraf plugins is incomplete', () => {
      const wrapper = setup({telegrafPlugins: [telegrafPlugin]})
      const skipLink = wrapper.find('[data-test="skip"]')

      expect(skipLink.exists()).toBe(false)
    })
  })

  describe('if type is line protocol', () => {
    it('renders back and next buttons with correct text', () => {
      const wrapper = setup({type: DataLoaderType.LineProtocol})
      const backButton = wrapper.find('[data-test="back"]')
      const nextButton = wrapper.find('[data-test="next"]')

      expect(backButton.prop('text')).toBe('Back to Admin Setup')
      expect(nextButton.prop('text')).toBe(
        'Continue to Line Protocol Configuration'
      )
      expect(nextButton.prop('status')).toBe(ComponentStatus.Default)
    })
  })

  describe('if type and substep is streaming', () => {
    describe('if there are no plugins selected', () => {
      it('renders streaming selector with buttons', () => {
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          params: {stepID: '2', substepID: 'streaming'},
        })
        const streamingSelector = wrapper.find(StreamingSelector)
        const backButton = wrapper.find('[data-test="back"]')
        const nextButton = wrapper.find('[data-test="next"]')

        expect(streamingSelector.exists()).toBe(true)
        expect(backButton.prop('text')).toBe('Back to Data Source Selection')
        expect(nextButton.prop('text')).toBe('Continue to Plugin Configuration')
        expect(nextButton.prop('status')).toBe(ComponentStatus.Disabled)
      })
    })

    describe('if there are plugins selected', () => {
      it('renders back and next button with correct text', () => {
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          params: {stepID: '2', substepID: 'streaming'},
          telegrafPlugins: [cpuTelegrafPlugin],
        })
        const backButton = wrapper.find('[data-test="back"]')
        const nextButton = wrapper.find('[data-test="next"]')

        expect(backButton.prop('text')).toBe('Back to Data Source Selection')
        expect(nextButton.prop('text')).toBe('Continue to Cpu')
        expect(nextButton.prop('status')).toBe(ComponentStatus.Default)
      })
    })

    describe('if back button is clicked', () => {
      it('calls prop functions as expected', () => {
        const onSetCurrentStepIndex = jest.fn()
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          params: {stepID: '2', substepID: 'streaming'},
          telegrafPlugins: [cpuTelegrafPlugin],
          onSetCurrentStepIndex,
        })
        const backButton = wrapper.find('[data-test="back"]')
        backButton.simulate('click')

        expect(onSetCurrentStepIndex).toBeCalledWith(2)
      })
    })

    describe('if next button is clicked', () => {
      it('calls prop functions as expected', () => {
        const onIncrementCurrentStepIndex = jest.fn()
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          params: {stepID: '2', substepID: 'streaming'},
          telegrafPlugins: [cpuTelegrafPlugin],
          onIncrementCurrentStepIndex,
        })
        const nextButton = wrapper.find('[data-test="next"]')
        nextButton.simulate('click')

        expect(onIncrementCurrentStepIndex).toBeCalled()
      })
    })
  })

  describe('if type is streaming but sub step is not', () => {
    describe('if next button is clicked', () => {
      it('calls prop functions as expected', () => {
        const onSetSubstepIndex = jest.fn()
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          params: {stepID: '2', substepID: undefined},
          telegrafPlugins: [cpuTelegrafPlugin],
          onSetSubstepIndex,
        })
        const nextButton = wrapper.find('[data-test="next"]')
        nextButton.simulate('click')

        expect(onSetSubstepIndex).toBeCalledWith(2, 'streaming')
      })
    })
  })
})
