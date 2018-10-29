import React from 'react'

import {shallow} from 'enzyme'

import GraphOptionsFixFirstColumn from 'src/dashboards/components/GraphOptionsFixFirstColumn'

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
      const checkbox = wrapper.find('input')

      expect(label.exists()).toBe(true)
      expect(checkbox.exists()).toBe(true)
      expect(checkbox.prop('type')).toBe('checkbox')
    })

    describe('if fixed is true', () => {
      it('input is checked', () => {
        const {wrapper} = setup()
        const checkbox = wrapper.find('input')

        expect(checkbox.prop('checked')).toBe(true)
      })
    })

    describe('if fixed is false', () => {
      it('input is not checked', () => {
        const {wrapper} = setup({fixed: false})
        const checkbox = wrapper.find('input')

        expect(checkbox.prop('checked')).toBe(false)
      })
    })
  })
})
