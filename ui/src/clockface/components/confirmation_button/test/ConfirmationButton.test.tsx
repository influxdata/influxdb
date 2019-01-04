import React from 'react'
import {mount} from 'enzyme'

import ConfirmationButton from 'src/clockface/components/confirmation_button/ConfirmationButton'
import Button from 'src/clockface/components/Button'

describe('ConfirmationButton', () => {
  let wrapper

  const wrapperSetup = (override = {}) => {
    const props = {
      text: 'I am a dangerous button!',
      confirmText: 'Click me if you dare',
      onConfirm: () => {},
      ...override,
    }

    return mount(<ConfirmationButton {...props} />)
  }

  it('matches snapshot with minimal props', () => {
    expect(wrapper).toMatchSnapshot()
  })

  it('renders correctly', () => {
    wrapper = wrapperSetup()
    expect(wrapper.find(ConfirmationButton)).toHaveLength(1)
  })

  describe('interaction', () => {
    it('shows the tooltip when clicked', () => {
      wrapper = wrapperSetup()

      wrapper.find(Button).simulate('click')

      expect(wrapper).toMatchSnapshot()
    })

    it('returns the specified value on tooltip click', () => {
      const onConfirm = jest.fn(x => x)
      const returnValue = 666

      wrapper = wrapperSetup({returnValue, onConfirm})

      wrapper.find(Button).simulate('click')

      wrapper
        .find({'data-test': 'confirmation-button--click-target'})
        .simulate('click')

      expect(onConfirm.mock.results[0].value).toBe(returnValue)
    })
  })
})
