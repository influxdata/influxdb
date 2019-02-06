// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import MultipleInput from './MultipleInput'
import MultipleRows from './MultipleRows'

import {TelegrafPluginInputCpu} from '@influxdata/influx'

const setup = (override = {}) => {
  const props = {
    title: '',
    helpText: '',
    onAddRow: jest.fn(),
    onDeleteRow: jest.fn(),
    onEditRow: jest.fn(),
    autoFocus: true,
    tags: [],
    onSetConfigArrayValue: jest.fn(),
    telegrafPluginName: TelegrafPluginInputCpu.NameEnum.Cpu,
    ...override,
  }

  const wrapper = shallow(<MultipleInput {...props} />)

  return {wrapper}
}

describe('Clockface.Components.MultipleInput', () => {
  it('renders', () => {
    const fieldName = 'yo'
    const {wrapper} = setup({fieldName})
    const multipleRows = wrapper.find(MultipleRows)

    expect(wrapper.exists()).toBe(true)
    expect(multipleRows.exists()).toBe(true)
  })
})
