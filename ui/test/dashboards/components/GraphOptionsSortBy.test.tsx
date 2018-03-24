import React from 'react'

import {mount} from 'enzyme'

import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import {Dropdown} from 'src/shared/components/Dropdown'

const defaultProps = {
  onChooseSortBy: () => {},
  selected: {
    displayName: '',
    internalName: '',
    visible: true,
  },
  sortByOptions: [],
}

const setup = (override = {}) => {
  const props = {...defaultProps, ...override}
  const wrapper = mount(<GraphOptionsSortBy {...props} />)

  return {wrapper, props}
}

describe('Dashboards.Components.GraphOptionsSortBy', () => {
  describe('rendering', () => {
    it('renders component', () => {
      const selected = {
        displayName: 'here',
        internalName: 'boom',
        visible: true,
      }
      const {wrapper} = setup({
        selected,
      })

      const dropdown = wrapper.find(Dropdown)
      const label = wrapper.find('label')

      expect(dropdown.props().selected).toEqual(selected.displayName)
      expect(dropdown.exists()).toBe(true)
      expect(label.exists()).toBe(true)
    })

    describe('when selected display name is not available', () => {
      it('render internal name as selected', () => {
        const selected = {
          displayName: '',
          internalName: 'boom',
          visible: true,
        }
        const {wrapper} = setup({selected})

        const dropdown = wrapper.find(Dropdown)

        expect(dropdown.props().selected).toEqual(selected.internalName)
      })
    })
  })
})
