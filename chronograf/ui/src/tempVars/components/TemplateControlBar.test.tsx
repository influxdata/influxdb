import React from 'react'
import {shallow} from 'enzyme'

import TemplateControlBar from 'src/tempVars/components/TemplateControlBar'
import TemplateControl from 'src/tempVars/components/TemplateControl'
import TemplateVariableEditor from 'src/tempVars/components/TemplateVariableEditor'
import OverlayTechnology from 'src/reusable_ui/components/overlays/OverlayTechnology'
import {source} from 'mocks/dummy'

import {TemplateType, TemplateValueType} from 'src/types'

const defaultProps = {
  isOpen: true,
  templates: [],
  meRole: 'EDITOR',
  isUsingAuth: true,
  onPickTemplate: () => {},
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
          localSelected: false,
        },
        {
          value: 'secondValue',
          type: TemplateValueType.Constant,
          selected: false,
          localSelected: false,
        },
      ],
    }
    const props = {...defaultProps, templates: [template]}
    const wrapper = shallow(<TemplateControlBar {...props} />)

    const dropdown = wrapper.find(TemplateControl)
    expect(dropdown.exists()).toBe(true)
  })

  it('renders component without variables', () => {
    const props = {...defaultProps}
    const wrapper = shallow(<TemplateControlBar {...props} />)

    const emptyState = wrapper.find({'data-test': 'empty-state'})

    const dropdown = wrapper.find(TemplateControl)

    expect(dropdown.exists()).toBe(false)
    expect(emptyState.exists()).toBe(true)
  })

  it('renders an TemplateVariableEditor overlay when adding a template variable', () => {
    const props = {...defaultProps}
    const wrapper = shallow(<TemplateControlBar {...props} />, {
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

    wrapper.find('[data-test="add-template-variable"]').simulate('click')

    const elements = wrapper
      .find(OverlayTechnology)
      .dive()
      .find(TemplateVariableEditor)

    expect(elements).toHaveLength(1)
  })
})
