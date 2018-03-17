import React from 'react'
import {shallow} from 'enzyme'

import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import Dropdown from 'src/shared/components/Dropdown'

const defaultProps = {
  sortByOptions: [],
  onChooseSortBy: () => {},
  selected: ''
}

const setup = (override = {}) => {
  const props = {...defaultProps, ...override}
  const wrapper = shallow(<GraphOptionsSortBy {...props} />)

  return {wrapper, props}
}

describe('Dashboards.Components.GraphOptionsSortBy', () => {
  describe('rendering', () => {
    it('renders component', () => {
      const {wrapper} = setup()

      const dropdown = wrapper.find(Dropdown)
      const label = wrapper.find('label')

      expect(dropdown.exists()).toBe(true)
      expect(label.exists()).toBe(true)
    })
  })
})
