import React from 'react'
import {shallow} from 'enzyme'

import {source} from 'mocks/dummy'
import TextTemplateBuilder from 'src/tempVars/components/TextTemplateBuilder'

import {TemplateType, TemplateValueType} from 'src/types'

describe('TextTemplateBuilder', () => {
  test('updates template approriately after edits', () => {
    const onUpdateTemplateMock = jest.fn()
    const template = {
      id: '0',
      tempVar: ':my-var:',
      label: '',
      type: TemplateType.Text,
      values: [
        {
          value: 'initial value',
          type: TemplateValueType.Constant,
          selected: true,
          localSelected: true,
        },
      ],
    }

    const props = {
      template,
      templates: [template],
      source,
      onUpdateTemplate: onUpdateTemplateMock,
      onUpdateDefaultTemplateValue: () => {},
    }

    const wrapper = shallow(<TextTemplateBuilder {...props} />)

    wrapper.find('input').simulate('change', {target: {value: 'new value'}})

    expect(onUpdateTemplateMock).toBeCalledWith({
      id: '0',
      tempVar: ':my-var:',
      label: '',
      type: TemplateType.Text,
      values: [
        {
          value: 'new value',
          type: TemplateValueType.Constant,
          selected: true,
          localSelected: true,
        },
      ],
    })
  })
})
