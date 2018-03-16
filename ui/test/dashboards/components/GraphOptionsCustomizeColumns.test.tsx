import React from 'react'

import GraphOptionsCustomizeColumns from 'src/dashboards/components/GraphOptionsCustomizeColumns'
import GraphOptionsCustomizableColumn from 'src/dashboards/components/GraphOptionsCustomizableColumn'
import {TIME_COLUMN_DEFAULT} from 'src/shared/constants/tableGraph'

import {shallow} from 'enzyme'

const setup = (override = {}) => {
  const props = {
    columns: [],
    onColumnUpdate: () => {},
    ...override,
  }

  const wrapper = shallow(<GraphOptionsCustomizeColumns {...props} />)

  return {wrapper, props}
}

describe('Dashboards.Components.GraphOptionsCustomizeColumns', () => {
  describe('rendering', () => {
    it('displays label and all columns passed in', () => {
      const columns = [TIME_COLUMN_DEFAULT]
      const {wrapper} = setup({columns})
      const label = wrapper.find('label')
      const customizableColumns = wrapper.find(GraphOptionsCustomizableColumn)

      expect(label.exists()).toBe(true)
      expect(customizableColumns.exists()).toBe(true)
      expect(customizableColumns.length).toBe(columns.length)
    })
  })
})
