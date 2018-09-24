import React from 'react'
import {shallow} from 'enzyme'

import WizardController from 'src/clockface/components/wizard/WizardController'
import WizardStep from 'src/clockface/components/wizard/WizardStep'
import WizardProgressBar from 'src/clockface/components/wizard/WizardProgressBar'

describe('WizardController', () => {
  let wrapper

  const wrapperSetup = (override = {}) => {
    const props = {
      children: null,
      handleSkip: undefined,
      skipLinkText: undefined,
      ...override,
    }

    return shallow(<WizardController {...props} />)
  }

  const childSetup = (override = {}) => {
    const props = {
      children: 'thing',
      title: 'title of thing',
      isComplete: () => false,
      isErrored: undefined,
      onPrevious: undefined,
      onNext: undefined,
      increment: undefined,
      decrement: undefined,
      tipText: undefined,
      nextLabel: undefined,
      previousLabel: undefined,
      isBlockingStep: undefined,
      lastStep: undefined,
      ...override,
    }

    return <WizardStep {...props} />
  }

  describe('with one child', () => {
    const wizardChild = childSetup({
      children: 'only step child',
      title: 'only wizard step',
    })

    beforeEach(() => {
      jest.resetAllMocks()
      wrapper = wrapperSetup({children: wizardChild})
    })

    it('mounts without exploding', () => {
      expect(wrapper).toHaveLength(1)
    })

    it('renders one WizardProgressBar component', () => {
      expect(wrapper.find(WizardProgressBar)).toHaveLength(1)
    })

    it('renders the first wizard step', () => {
      const currentStep = wrapper.find(WizardStep)

      expect(currentStep).toHaveLength(1)
      expect(currentStep.props().title).toBe('only wizard step')
    })

    it('matches snapshot with one child', () => {
      expect(wrapper).toMatchSnapshot()
    })
  })

  describe('with multiple children', () => {
    const wizardChildren = [
      childSetup({
        children: 'step child 1',
        title: 'wizard step 1',
      }),
      childSetup({
        children: 'step child 2',
        title: 'wizard step 2',
        lastStep: true,
      }),
    ]

    beforeEach(() => {
      jest.resetAllMocks()
      wrapper = wrapperSetup({children: wizardChildren})
    })

    it('mounts without exploding', () => {
      expect(wrapper).toHaveLength(1)
    })

    it('renders one WizardProgressBar component', () => {
      expect(wrapper.find(WizardProgressBar)).toHaveLength(1)
    })

    it('renders the first wizard step', () => {
      const currentStep = wrapper.find(WizardStep)

      expect(currentStep).toHaveLength(1)
      expect(currentStep.props().title).toBe('wizard step 1')
    })

    it('increments the currentStepIndex when incrementStep is invoked', () => {
      expect(wrapper.state().currentStepIndex).toBe(0)
      wrapper.instance().incrementStep()
      expect(wrapper.state().currentStepIndex).toBe(1)
    })

    it('jumps to specific index when jumpToStep is invoked with a valid number parameter', () => {
      expect(wrapper.state().currentStepIndex).toBe(0)
      wrapper.instance().jumpToStep(1)()
      expect(wrapper.state().currentStepIndex).toBe(1)
    })

    it('matches snapshot with two children', () => {
      expect(wrapper).toMatchSnapshot()
    })

    describe('with first step complete', () => {
      const completeChildren = [
        childSetup({
          isComplete: () => true,
          children: 'complete step child 1',
          title: 'complete wizard step 1',
        }),
        childSetup({
          children: 'step child 2',
          title: 'wizard step 2',
          lastStep: true,
        }),
      ]

      beforeEach(() => {
        jest.resetAllMocks()
        wrapper = wrapperSetup({children: completeChildren})
      })

      it('renders step two when step one is complete', () => {
        const currentStep = wrapper.find(WizardStep)

        expect(currentStep).toHaveLength(1)
        expect(currentStep.props().title).toBe('wizard step 2')
      })

      it('increments the currentStepIndex when decrementStep is invoked', () => {
        expect(wrapper.state().currentStepIndex).toBe(1)
        wrapper.instance().decrementStep()
        expect(wrapper.state().currentStepIndex).toBe(0)
      })

      it('matches snapshot with first step complete', () => {
        expect(wrapper).toMatchSnapshot()
      })
    })
  })
})
