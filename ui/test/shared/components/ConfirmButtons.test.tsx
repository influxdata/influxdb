import React from 'react'
import ConfirmButtons, {
  Confirm,
  Cancel,
} from 'src/shared/components/ConfirmButtons'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    item: '',
    buttonSize: '',
    isDisabled: false,
    confirmLeft: false,
    confirmTitle: '',
    onConfirm: () => {},
    onCancel: () => {},
    onClickOutside: () => {},
    ...override,
  }

  const wrapper = shallow(<ConfirmButtons {...props} />)

  return {
    props,
    wrapper,
  }
}

describe('Componenets.Shared.ConfirmButtons', () => {
  describe('rendering', () => {
    it('has a confirm and cancel button', () => {
      const {wrapper} = setup()
      const confirm = wrapper.dive().find(Confirm)
      const cancel = wrapper.dive().find(Cancel)

      expect(confirm.exists()).toBe(true)
      expect(cancel.exists()).toBe(true)
    })

    describe('confirmLeft is true', () => {
      it('has a confirm button to the left of the cancel button', () => {
        const {wrapper} = setup({confirmLeft: true})
        const buttons = wrapper.dive().children()
        const firstButton = buttons.first()
        const lastButton = buttons.last()

        expect(firstButton.find(Confirm).exists()).toBe(true)
        expect(lastButton.find(Cancel).exists()).toBe(true)
      })
    })

    describe('confirmLeft is false', () => {
      it('has a confirm button to the right of the cancel button', () => {
        const {wrapper} = setup({confirmLeft: false})
        const buttons = wrapper.dive().children()
        const firstButton = buttons.first()
        const lastButton = buttons.last()

        expect(firstButton.find(Cancel).exists()).toBe(true)
        expect(lastButton.find(Confirm).exists()).toBe(true)
      })
    })

    describe('confirmTitle', () => {
      describe('is not defined', () => {
        it('has a default title', () => {
          const {wrapper} = setup({confirmTitle: undefined})
          const confirm = wrapper.dive().find(Confirm)
          const title = confirm.dive().props().title

          expect(title.length).toBeTruthy()
        })
      })

      describe('is defined', () => {
        it('has the title passed in as props', () => {
          const confirmTitle = 'delete'
          const {wrapper} = setup({confirmTitle})
          const confirm = wrapper.dive().find(Confirm)
          const title = confirm.dive().props().title

          expect(title).toContain(confirmTitle)
        })
      })
    })
  })

  describe('user interaction', () => {
    describe('when clicking confirm', () => {
      it('should fire onConfirm', () => {
        const onConfirm = jest.fn()
        const item = 'test-item'
        const {wrapper} = setup({onConfirm, item})
        const confirm = wrapper.dive().find(Confirm)
        const button = confirm.dive().find({'data-test': 'confirm'})
        button.simulate('click')

        expect(onConfirm).toHaveBeenCalledTimes(1)
        expect(onConfirm).toHaveBeenCalledWith(item)
      })
    })

    describe('when clicking cancel', () => {
      it('should fire onCancel', () => {
        const onCancel = jest.fn()
        const item = 'test-item'
        const {wrapper} = setup({onCancel, item})
        const cancel = wrapper.dive().find(Cancel)
        const button = cancel.dive().find({'data-test': 'cancel'})
        button.simulate('click')

        expect(onCancel).toHaveBeenCalledTimes(1)
        expect(onCancel).toHaveBeenCalledWith(item)
      })
    })
  })
})
