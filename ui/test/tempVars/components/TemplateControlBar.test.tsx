import React from 'react'
import {shallow} from 'enzyme'

import TemplateControlBar from 'src/tempVars/components/TemplateControlBar'
import TemplateControlDropdown from 'src/tempVars/components/TemplateControlDropdown'
import {TemplateType, TemplateValueType} from 'src/types'
import {source} from 'test/resources'

const defaultProps = {
  isOpen: true,
  templates: [
    {
      id: '000',
      tempVar: ':alpha:',
      label: '',
      type: TemplateType.Constant,
      values: [
        {
          value: 'firstValue',
          type: TemplateValueType.Constant,
          selected: false,
        },
        {
          value: 'secondValue',
          type: TemplateValueType.Constant,
          selected: false,
        },
      ],
    },
  ],
  meRole: 'EDITOR',
  isUsingAuth: true,
  onOpenTemplateManager: () => {},
  onSelectTemplate: () => {},
  onSaveTemplates: () => {},
  onCreateTemplateVariable: () => {},
  source,
}

const setup = (override = {}) => {
  const props = {...defaultProps, ...override}
  const wrapper = shallow(<TemplateControlBar {...props} />)

  return {wrapper, props}
}

describe('Dashboard.TemplateControlBar', () => {
  describe('rendering', () => {
    it('renders component with variables', () => {
      const {wrapper} = setup()

      const dropdown = wrapper.find(TemplateControlDropdown)
      expect(dropdown.exists()).toBe(true)
    })

    it('renders component without variables', () => {
      const {wrapper} = setup({...defaultProps, templates: []})

      const emptyState = wrapper.find({'data-test': 'empty-state'})

      const dropdown = wrapper.find(TemplateControlDropdown)

      expect(dropdown.exists()).toBe(false)
      expect(emptyState.exists()).toBe(true)
    })
  })
})
