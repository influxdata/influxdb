import React from 'react'

import GraphOptionsFixFirstColumn from 'src/dashboards/components/GraphOptionsFixFirstColumn'
import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    fixed: true,
    onToggleFixFirstColumn: () => {},
    ...override,
  }

  const wrapper = shallow(<GraphOptionsFixFirstColumn {...props} />)
  return {wrapper, props}
}

describe('Dashboards.Components.GraphOptionsFixFirstColumn', () => {
  describe('rendering', () => {
    it('shows checkbox and label', () => {
      const {wrapper} = setup()
      const label = wrapper.find('label')
      const checkbox = wrapper.find({'data-test': 'checkbox'})

      expect(label.exists()).toBe(true)
      expect(checkbox.exists()).toBe(true)
    })
  })
})
