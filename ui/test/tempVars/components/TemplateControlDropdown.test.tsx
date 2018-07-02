import React from 'react'
import {shallow} from 'enzyme'

import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import TemplateVariableEditor from 'src/tempVars/components/TemplateVariableEditor'
import TemplateControlDropdown from 'src/tempVars/components/TemplateControlDropdown'
import {source} from 'test/resources'

import {TemplateType, TemplateValueType} from 'src/types'

const template = {
  id: '0',
  tempVar: ':my-var:',
  label: '',
  type: TemplateType.Databases,
  values: [
    {
      value: 'db0',
      type: TemplateValueType.Database,
      selected: true,
      localSelected: true,
    },
  ],
}

const defaultProps = {
  template,
  templates: [template],
  meRole: 'EDITOR',
  isUsingAuth: true,
  source,
  onPickTemplate: () => Promise.resolve(),
  onCreateTemplate: () => Promise.resolve(),
  onUpdateTemplate: () => Promise.resolve(),
  onDeleteTemplate: () => Promise.resolve(),
}

describe('TemplateControlDropdown', () => {
  it('should show a TemplateVariableEditor overlay when the settings icon is clicked', () => {
    const wrapper = shallow(<TemplateControlDropdown {...defaultProps} />, {
      context: {
        store: {},
      },
    })

    const children = wrapper
      .find(OverlayTechnology)
      .dive()
      .find("[data-test='overlay-children']")
      .children()

    expect(children).toHaveLength(0)

    wrapper.find("[data-test='edit']").simulate('click')

    const elements = wrapper
      .find(OverlayTechnology)
      .dive()
      .find(TemplateVariableEditor)

    expect(elements).toHaveLength(1)
  })
})
