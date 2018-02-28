import React from 'react'
import Dropdown from 'shared/components/Dropdown'
import DropdownMenu from 'shared/components/DropdownMenu'

import {shallow} from 'enzyme'
const items = [{text: 'foo'}, {text: 'bar'}]

const setup = (override = {}) => {
  const props = {
    items: [],
    selected: '',
    onChoose: jest.fn(),
    ...override,
  }

  const dropdown = shallow(<Dropdown {...props} />).dive({
    'data-test': 'dropdown-button',
  })

  return {
    dropdown,
    props,
  }
}

describe('Components.Shared.Dropdown', () => {
  describe('rednering', () => {
    describe('initial render', () => {
      it('renders the dropdown menu button', () => {
        const {dropdown} = setup()

        expect(dropdown.exists()).toBe(true)
      })

      it('does not show the list', () => {
        const {dropdown} = setup({items})

        const menu = dropdown.find(DropdownMenu)
        expect(menu.exists()).toBe(false)
      })
    })

    describe('user interactions', () => {
      it('shows the menu when clicked', () => {
        const {dropdown} = setup({items})

        dropdown.simulate('click')

        const menu = dropdown.find(DropdownMenu)
        expect(menu.exists()).toBe(true)
      })
    })
  })
})
