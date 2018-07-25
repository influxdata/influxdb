import React from 'react'
import InputClickToEdit from 'src/shared/components/InputClickToEdit'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    appearAsNormalInput: false,
    disabled: false,
    onBlur: () => {},
    onChange: () => {},
    placeholder: '',
    tabIndex: 0,
    value: '',
    wrapperClass: '',
    ...override,
  }

  const defaultState = {
    initialValue: '',
    isEditing: false,
  }

  const inputClickToEdit = shallow(<InputClickToEdit {...props} />)
  return {
    defaultState,
    inputClickToEdit,
    props,
  }
}

describe('Components.Shared.InputClickToEdit', () => {
  describe('rendering', () => {
    it('does not display input by default', () => {
      const {inputClickToEdit} = setup()
      const initialDiv = inputClickToEdit.children().find('div')
      const inputField = inputClickToEdit.children().find('input')
      const disabledDiv = inputClickToEdit
        .children()
        .find({'data-test': 'disabled'})

      expect(initialDiv.exists()).toBe(true)
      expect(inputField.exists()).toBe(false)
      expect(disabledDiv.exists()).toBe(false)
    })

    describe('if disabled passed in as props is true', () => {
      it('should show the disabled div', () => {
        const disabled = true
        const {inputClickToEdit} = setup({disabled})
        const inputField = inputClickToEdit.children().find('input')
        const disabledDiv = inputClickToEdit
          .children()
          .find({'data-test': 'disabled'})

        expect(inputField.exists()).toBe(false)
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
        const initialDiv = inputClickToEdit.children().find('div')
        initialDiv.simulate('click')
        const divAfterClick = inputClickToEdit.children().find('div')
        const inputField = inputClickToEdit.children().find('input')
        const isEditing = inputClickToEdit.state('isEditing')

        expect(isEditing).toBe(true)
        expect(divAfterClick.exists()).toBe(false)
        expect(inputField.exists()).toBe(true)
      })
    })
  })
})
