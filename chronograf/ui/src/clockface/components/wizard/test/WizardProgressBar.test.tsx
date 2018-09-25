import React from 'react'
import {shallow} from 'enzyme'

import WizardProgressBar from 'src/clockface/components/wizard/WizardProgressBar'
import ProgressConnector from 'src/clockface/components/wizard/ProgressConnector'

import {StepStatus} from 'src/clockface/constants/wizard'

describe('Wizard Progress Bar', () => {
  let wrapper

  const setup = (override = {}, stepStatuses = [false, false, false]) => {
    const step1 = {
      title: 'this is step1',
      stepStatus: stepStatuses[0] ? StepStatus.Complete : StepStatus.Incomplete,
    }
    const step2 = {
      title: 'this is step2',
      stepStatus: stepStatuses[1] ? StepStatus.Complete : StepStatus.Incomplete,
    }
    const step3 = {
      title: 'this is step3',
      stepStatus: stepStatuses[2] ? StepStatus.Complete : StepStatus.Incomplete,
    }
    const Props = {
      steps: [step1, step2, step3],
      currentStepIndex: 0,
      handleJump: jest.fn(),
      ...override,
    }
    return shallow(<WizardProgressBar {...Props} />)
  }

  describe('when no steps are complete', () => {
    beforeEach(() => {
      wrapper = setup()
    })

    it('mounts without exploding', () => {
      expect(wrapper).toHaveLength(1)
    })

    it('renders two progress connectors when rendered with three steps', () => {
      expect(wrapper.find(ProgressConnector)).toHaveLength(2)
    })

    it('renders three incomplete step indicator ', () => {
      expect(wrapper.find('.circle-thick')).toHaveLength(3)
    })

    it('renders one current step indicator icon', () => {
      expect(wrapper.find('.current')).toHaveLength(1)
    })

    it('matches snapshot', () => {
      expect(wrapper).toMatchSnapshot()
    })
  })

  describe('when the first two steps are completed', () => {
    beforeEach(() => {
      wrapper = setup({currentStepIndex: 2}, [true, true, false])
    })

    it('renders three checkmark icons', () => {
      expect(wrapper.find('.checkmark')).toHaveLength(3)
    })

    it('renders no current step indicator icons', () => {
      expect(wrapper.find('.current')).toHaveLength(0)
    })

    it('matches snapshot', () => {
      expect(wrapper).toMatchSnapshot()
    })
  })
})
