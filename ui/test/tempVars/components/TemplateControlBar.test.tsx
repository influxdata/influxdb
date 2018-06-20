import React from 'react'
import {shallow} from 'enzyme'

import TemplateControlBar from 'src/tempVars/components/TemplateControlBar'
import TemplateControlDropdown from 'src/tempVars/components/TemplateControlDropdown'
import {TemplateType, TemplateValueType} from 'src/types'
import {source} from 'test/resources'

const defaultProps = {
  isOpen: true,
  templates: [],
  meRole: 'EDITOR',
  isUsingAuth: true,
  onSelectTemplate: () => {},
  onSaveTemplates: () => {},
  onCreateTemplateVariable: () => {},
  source,
}

describe('TemplateControlBar', () => {
  it('renders component with variables', () => {
    const template = {
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
    }
    const props = {...defaultProps, templates: [template]}
    const wrapper = shallow(<TemplateControlBar {...props} />)

    const dropdown = wrapper.find(TemplateControlDropdown)
    expect(dropdown.exists()).toBe(true)
  })

  it('renders component without variables', () => {
    const props = {...defaultProps}
    const wrapper = shallow(<TemplateControlBar {...props} />)

    const emptyState = wrapper.find({'data-test': 'empty-state'})

    const dropdown = wrapper.find(TemplateControlDropdown)

    expect(dropdown.exists()).toBe(false)
    expect(emptyState.exists()).toBe(true)
  })
})
