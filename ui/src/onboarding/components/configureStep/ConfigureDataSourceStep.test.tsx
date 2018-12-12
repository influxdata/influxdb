// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {ConfigureDataSourceStep} from 'src/onboarding/components/configureStep/ConfigureDataSourceStep'
import ConfigureDataSourceSwitcher from 'src/onboarding/components/configureStep/ConfigureDataSourceSwitcher'
import {Button} from 'src/clockface'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

// Dummy Data
import {
  defaultOnboardingStepProps,
  cpuTelegrafPlugin,
  redisTelegrafPlugin,
  diskTelegrafPlugin,
} from 'mocks/dummyData'

const setup = (override = {}) => {
  const props = {
    ...defaultOnboardingStepProps,
    telegrafPlugins: [],
    onSetActiveTelegrafPlugin: jest.fn(),
    onUpdateTelegrafPluginConfig: jest.fn(),
    onSetPluginConfiguration: jest.fn(),
    type: DataLoaderType.Empty,
    onAddConfigValue: jest.fn(),
    onRemoveConfigValue: jest.fn(),
    onSaveTelegrafConfig: jest.fn(),
    authToken: '',
    params: {
      stepID: '3',
      substepID: '0',
    },
    location: null,
    router: null,
    routes: [],
    ...override,
  }

  return shallow(<ConfigureDataSourceStep {...props} />)
}

describe('Onboarding.Components.ConfigureStep.ConfigureDataSourceStep', () => {
  it('renders switcher and buttons', async () => {
    const wrapper = setup()
    const switcher = wrapper.find(ConfigureDataSourceSwitcher)
    const buttons = wrapper.find(Button)

    expect(wrapper.exists()).toBe(true)
    expect(switcher.exists()).toBe(true)
    expect(buttons.length).toBe(3)
  })

  describe('if type is not streaming', () => {
    it('renders back and next buttons with correct text', () => {
      const wrapper = setup({type: DataLoaderType.LineProtocol})
      const backButton = wrapper.find('[data-test="back"]')
      const nextButton = wrapper.find('[data-test="next"]')

      expect(backButton.prop('text')).toBe('Back to Select Data Source Type')
      expect(nextButton.prop('text')).toBe('Continue to Verify')
    })
  })

  describe('if type is streaming', () => {
    describe('if the substep is 0', () => {
      it('renders back button with correct text', () => {
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          params: {stepID: '3', substepID: '0'},
        })
        const backButton = wrapper.find('[data-test="back"]')

        expect(backButton.prop('text')).toBe('Back to Select Streaming Sources')
      })

      describe('when the back button is clicked', () => {
        it('calls prop functions as expected', () => {
          const onSetActiveTelegrafPlugin = jest.fn()
          const onSetSubstepIndex = jest.fn()
          const wrapper = setup({
            type: DataLoaderType.Streaming,
            telegrafPlugins: [cpuTelegrafPlugin],
            params: {stepID: '3', substepID: '0'},
            onSetActiveTelegrafPlugin,
            onSetSubstepIndex,
          })
          const backButton = wrapper.find('[data-test="back"]')
          backButton.simulate('click')

          expect(onSetSubstepIndex).toBeCalledWith(2, 'streaming')
        })
      })
    })

    describe('if its the last stubstep', () => {
      it('renders the next button with correct text', () => {
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          telegrafPlugins: [cpuTelegrafPlugin],
          params: {stepID: '3', substepID: '1'},
        })
        const nextButton = wrapper.find('[data-test="next"]')

        expect(nextButton.prop('text')).toBe('Continue to Verify')
      })
    })

    describe('if its the neither the last or firt stubstep', () => {
      it('renders the next and back buttons with correct text', () => {
        const wrapper = setup({
          type: DataLoaderType.Streaming,
          telegrafPlugins: [
            cpuTelegrafPlugin,
            redisTelegrafPlugin,
            diskTelegrafPlugin,
          ],
          params: {stepID: '3', substepID: '1'},
        })
        const nextButton = wrapper.find('[data-test="next"]')
        const backButton = wrapper.find('[data-test="back"]')

        expect(nextButton.prop('text')).toBe('Continue to Disk')
        expect(backButton.prop('text')).toBe('Back to Cpu')
      })

      describe('when the back button is clicked', () => {
        it('calls prop functions as expected', () => {
          const onSetActiveTelegrafPlugin = jest.fn()
          const onSetSubstepIndex = jest.fn()
          const wrapper = setup({
            type: DataLoaderType.Streaming,
            telegrafPlugins: [
              cpuTelegrafPlugin,
              redisTelegrafPlugin,
              diskTelegrafPlugin,
            ],
            params: {stepID: '3', substepID: '1'},
            onSetActiveTelegrafPlugin,
            onSetSubstepIndex,
          })
          const backButton = wrapper.find('[data-test="back"]')
          backButton.simulate('click')

          expect(onSetActiveTelegrafPlugin).toBeCalledWith('cpu')
          expect(onSetSubstepIndex).toBeCalledWith(3, 0)
        })
      })

      describe('when the next button is clicked', () => {
        it('calls prop functions as expected', () => {
          const onSetActiveTelegrafPlugin = jest.fn()
          const onSetSubstepIndex = jest.fn()
          const wrapper = setup({
            type: DataLoaderType.Streaming,
            telegrafPlugins: [
              cpuTelegrafPlugin,
              redisTelegrafPlugin,
              diskTelegrafPlugin,
            ],
            params: {stepID: '3', substepID: '1'},
            onSetActiveTelegrafPlugin,
            onSetSubstepIndex,
          })
          const nextButton = wrapper.find('[data-test="next"]')
          nextButton.simulate('click')

          expect(onSetActiveTelegrafPlugin).toBeCalledWith('disk')
          expect(onSetSubstepIndex).toBeCalledWith(3, 2)
        })
      })
    })
  })
})
