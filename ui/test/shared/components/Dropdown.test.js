import React from 'react'
import Dropdown from 'shared/components/dropdown'
import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    items: [],
    selected: '',
    onChoose: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<Dropdown {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Components.Shared.Dropdown', () => {
  describe('rednering', () => {
    describe('initial render', () => {
      it('renders the dropdown button', () => {
        const {wrapper} = setup()

        const actual = wrapper.dive({'data-test': 'dropdown-button'})
        expect(actual.length).toBe(1)
      })

      it('does not show the list', () => {
        const items = [{text: 'foo'}, {text: 'bar'}]
        const {wrapper} = setup({items})

        const dropdown = wrapper.dive({'data-test': 'dropdown-button'})
        const list = dropdown.find({'data-test': 'dropdown-items'})
        expect(list.length).toBe(0)
      })
    })

    describe('user interactions', () => {
      it('shows the list when clicked', () => {
        const items = [{text: 'foo'}, {text: 'bar'}]
        const {wrapper} = setup({items})

        const dropdown = wrapper.dive({'data-test': 'dropdown-button'})
        dropdown.simulate('click')

        const ul = dropdown.find({'data-test': 'dropdown-ul'})
        expect(ul.length).toBe(1)

        const list = ul.find({'data-test': 'dropdown-item'})
        expect(list.length).toBe(items.length)
      })
    })
  })
})
