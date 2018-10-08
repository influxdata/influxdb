import React from 'react'

import {mount} from 'enzyme'

import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'

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

      const selectedText = wrapper.find('.dropdown-selected').text()

      expect(selectedText).toEqual(selected.displayName)
    })

    describe('when selected display name is not available', () => {
      it('render internal name as selected', () => {
        const selected = {
          displayName: '',
          internalName: 'boom',
          visible: true,
        }

        const {wrapper} = setup({selected})

        const selectedText = wrapper.find('.dropdown-selected').text()

        expect(selectedText).toEqual(selected.internalName)
      })
    })
  })
})
