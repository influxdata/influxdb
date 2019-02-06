// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import MultipleRows from './MultipleRows'

import {TelegrafPluginInputCpu} from '@influxdata/influx'

const setup = (override = {}) => {
  const props = {
    confirmText: '',
    onDeleteTag: jest.fn(),
    fieldName: '',
    tags: [],
    onSetConfigArrayValue: jest.fn(),
    telegrafPluginName: TelegrafPluginInputCpu.NameEnum.Cpu,
    ...override,
  }

  const wrapper = shallow(<MultipleRows {...props} />)

  return {wrapper}
}

describe('Clockface.Components.MultipleRows', () => {
  it('renders', () => {
    const fieldName = 'yo'
    const {wrapper} = setup({fieldName})

    expect(wrapper.exists()).toBe(true)
  })
})
