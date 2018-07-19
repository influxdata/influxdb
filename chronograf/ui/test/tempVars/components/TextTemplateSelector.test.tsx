import React from 'react'
import {shallow} from 'enzyme'

import TextTemplateSelector from 'src/tempVars/components/TextTemplateSelector'

import {TemplateType, TemplateValueType} from 'src/types'

describe('TextTemplateSelector', () => {
  test('updates template on enter', () => {
    const onPickValueMock = jest.fn()
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

    const wrapper = shallow(
      <TextTemplateSelector template={template} onPickValue={onPickValueMock} />
    )

    wrapper.find('input').simulate('change', {target: {value: 'new value'}})
    expect(wrapper.find('input').prop('value')).toEqual('new value')

    wrapper.find('input').simulate('keyup', {
      key: 'Enter',
      currentTarget: {
        blur() {
          wrapper.find('input').simulate('blur')
        },
      },
    })

    expect(onPickValueMock).toBeCalledWith({
      value: 'new value',
      type: TemplateValueType.Constant,
      selected: true,
      localSelected: true,
    })
  })
})
