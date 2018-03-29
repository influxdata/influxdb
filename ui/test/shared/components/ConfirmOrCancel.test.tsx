import React from 'react'
import ConfirmOrCancel, {
  Cancel,
  Confirm,
} from 'src/shared/components/ConfirmOrCancel'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    buttonSize: '',
    reversed: false,
    confirmTitle: '',
    cancelTitle: '',
    isDisabled: false,
    item: '',
    onCancel: () => {},
    onClickOutside: () => {},
    onConfirm: () => {},
    ...override,
  }

  const wrapper = shallow(<ConfirmOrCancel {...props} />)

  return {
    props,
    wrapper,
  }
}

describe('Componenets.Shared.ConfirmOrCancel', () => {
  describe('rendering', () => {
    it('has a confirm and cancel button', () => {
      const {wrapper} = setup()
      const confirm = wrapper.dive().find(Confirm)
      const cancel = wrapper.dive().find(Cancel)

      expect(confirm.exists()).toBe(true)
      expect(cancel.exists()).toBe(true)
    })

    describe('reversed is true', () => {
      it('has a confirm button to the left of the cancel button', () => {
        const {wrapper} = setup({reversed: true})
        const buttons = wrapper.dive().children()
        const firstButton = buttons.first()
        const lastButton = buttons.last()

        expect(firstButton.find(Confirm).exists()).toBe(true)
        expect(lastButton.find(Cancel).exists()).toBe(true)
      })
    })

    describe('reversed is false', () => {
      it('has a confirm button to the right of the cancel button', () => {
        const {wrapper} = setup({reversed: false})
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
          const title = confirm.prop('title')

          expect(title.length).toBeTruthy()
        })
      })

      describe('is defined', () => {
        it('has the title passed in as props', () => {
          const confirmTitle = 'delete'
          const {wrapper} = setup({confirmTitle})
          const confirm = wrapper.dive().find(Confirm)
          const title = confirm.prop('title')

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
