import React from 'react'
import {shallow} from 'enzyme'

import WizardButtonBar from 'src/reusable_ui/components/wizard/WizardButtonBar'

describe('WizardButtonBar', () => {
  let wrapper

  const props = {
    onClickPrevious: jest.fn(),
    onClickNext: jest.fn(),
    decrement: undefined,
    nextLabel: undefined,
    previousLabel: undefined,
    lastStep: undefined,
  }

  beforeEach(() => (wrapper = shallow(<WizardButtonBar {...props} />)))

  it('mounts without exploding', () => {
    expect(wrapper).toHaveLength(1)
  })

  it('renders a div', () => {
    expect(wrapper.find('div')).toHaveLength(1)
  })

  it('renders only the "Next" button if decrement is undefined', () => {
    const button = wrapper.find('button')
    expect(button).toHaveLength(1)
    expect(button.props().children).toBe('Next')
  })

  it('renders a primary button if lastStep is falsy', () => {
    const buttonColor = wrapper.instance().buttonColor
    expect(buttonColor).toBe('btn-primary')
  })

  it('matches snapshot when provided minimal props', () => {
    expect(wrapper).toMatchSnapshot()
  })

  describe('with decrement and last step is "true"', () => {
    const propsWithDecrement = {
      ...props,
      decrement: jest.fn(),
      lastStep: true,
    }

    beforeEach(() => {
      jest.resetAllMocks()
      wrapper = shallow(<WizardButtonBar {...propsWithDecrement} />)
    })

    it('renders the "Previous" button if decrement is defined and lastStep is true', () => {
      const button = wrapper.find('button')
      expect(button).toHaveLength(2)
      expect(button.at(0).props().children).toBe('Previous')
    })

    it('renders a success button if lastStep is true', () => {
      const buttonColor = wrapper.instance().buttonColor
      expect(buttonColor).toBe('btn-success')
    })

    it('matches snapshot when provided decrement function and last step as "true"', () => {
      expect(wrapper).toMatchSnapshot()
    })
  })
})
