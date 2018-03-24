import {shallow} from 'enzyme'
import React from 'react'

import GraphOptionsCustomizableField from 'src/dashboards/components/GraphOptionsCustomizableField'
import GraphOptionsCustomizeFields from 'src/dashboards/components/GraphOptionsCustomizeFields'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {TIME_FIELD_DEFAULT} from 'src/shared/constants/tableGraph'

const setup = (override = {}) => {
  const props = {
    fields: [],
    onFieldUpdate: () => {},
    ...override,
  }

  const wrapper = shallow(<GraphOptionsCustomizeFields {...props} />)

  return {wrapper, props}
}

describe('Dashboards.Components.GraphOptionsCustomizableField', () => {
  describe('rendering', () => {
    it('displays label and all fields passed in', () => {
      const fields = [TIME_FIELD_DEFAULT]
      const {wrapper} = setup({fields})
      const label = wrapper.find('label')
      const customizableFields = wrapper.find(GraphOptionsCustomizableField)
      const Scrollbox = wrapper.find(FancyScrollbar)

      expect(label.exists()).toBe(true)
      expect(customizableFields.exists()).toBe(true)
      expect(customizableFields.length).toBe(fields.length)
      expect(Scrollbox.exists()).toBe(true)
    })
  })
})
