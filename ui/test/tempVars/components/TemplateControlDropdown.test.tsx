import React from 'react'
import {shallow} from 'enzyme'

import SimpleOverlayTechnology from 'src/shared/components/SimpleOverlayTechnology'
import TemplateVariableEditor from 'src/tempVars/components/TemplateVariableEditor'
import TemplateControlDropdown from 'src/tempVars/components/TemplateControlDropdown'
import {source} from 'test/resources'

import {TemplateType, TemplateValueType} from 'src/types'

const defaultProps = {
  template: {
    id: '0',
    tempVar: ':my-var:',
    label: '',
    type: TemplateType.Databases,
    values: [
      {
        value: 'db0',
        type: TemplateValueType.Database,
        selected: true,
        picked: true,
      },
    ],
  },
  meRole: 'EDITOR',
  isUsingAuth: true,
  source,
  onSelectTemplate: () => Promise.resolve(),
  onCreateTemplate: () => Promise.resolve(),
  onUpdateTemplate: () => Promise.resolve(),
  onDeleteTemplate: () => Promise.resolve(),
}

describe('TemplateControlDropdown', () => {
  it('should show a TemplateVariableEditor overlay when the settings icon is clicked', () => {
    const wrapper = shallow(<TemplateControlDropdown {...defaultProps} />)

    expect(wrapper.find(SimpleOverlayTechnology)).toHaveLength(0)

    wrapper.find("[data-test='edit']").simulate('click')

    const elements = wrapper
      .find(SimpleOverlayTechnology)
      .dive()
      .find(TemplateVariableEditor)

    expect(elements).toHaveLength(1)
  })
})
