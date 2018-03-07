import React from 'react'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    wrapperClass: '',
    value: '',
    onChange: () => {},
    onBlur: () => {},
    disabled: false,
    tabIndex: 0,
    placeholder: '',
    appearAsNormalInput: false,
    ...override,
  }

  const defaultState = {
    isEditing: false,
    initialValue: '',
  }

  const inputClickToEdit = shallow(<InputClickToEdit {...props} />)
  return {
    props,
    defaultState,
    inputClickToEdit,
  }
}

describe('Components.Shared.InputClickToEdit', () => {
  describe('rendering', () => {
    it('does not display input by default', () => {
      const {inputClickToEdit} = setup()
      const initialDiv = inputClickToEdit
        .children()
        .find({'data-test': 'unclicked'})
      const inputField = inputClickToEdit
        .children()
        .find({'data-test': 'input'})

      expect(initialDiv.exists()).toBe(true)
      expect(inputField.exists()).toBe(false)
    })

    describe('if disabled passed in as props is true', () => {
      it('should show the disabled div', () => {
        const disabled = true
        const {inputClickToEdit} = setup({disabled})
        const disabledDiv = inputClickToEdit
          .children()
          .find({'data-test': 'disabled'})

        expect(disabledDiv.exists()).toBe(true)
      })

      it('should not have an icon', () => {
        const disabled = true
        const {inputClickToEdit} = setup({disabled})
        const disabledDiv = inputClickToEdit
          .children()
          .find({'data-test': 'disabled'})
        const icon = disabledDiv.children().find({
          'data-test': 'icon',
        })

        expect(icon.exists()).toBe(false)
      })
    })
  })

  describe('user interaction', () => {
    describe('when clicking component', () => {
      it('should render input field', () => {
        const {inputClickToEdit} = setup()
        const initialDiv = inputClickToEdit
          .children()
          .find({'data-test': 'unclicked'})
        initialDiv.simulate('click')
        const divAfterClick = inputClickToEdit
          .children()
          .find({'data-test': 'unclicked'})
        const inputField = inputClickToEdit
          .children()
          .find({'data-test': 'input'})
        const isEditing = inputClickToEdit.state('isEditing')

        expect(isEditing).toBe(true)
        expect(divAfterClick.exists()).toBe(false)
        expect(inputField.exists()).toBe(true)
      })
    })
  })
})
