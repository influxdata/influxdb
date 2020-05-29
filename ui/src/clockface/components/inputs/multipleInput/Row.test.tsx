// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import Row from './Row'

import {TelegrafPluginInputCpu} from '@influxdata/influx'

const setup = (override = {}) => {
  const props = {
    confirmText: '',
    item: {},
    onDeleteTag: jest.fn(),
    onSetConfigArrayValue: jest.fn(),
    fieldName: '',
    telegrafPluginName: TelegrafPluginInputCpu.NameEnum.Cpu,
    index: 0,
    ...override,
  }

  const wrapper = shallow(<Row {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConfigureStep.Streaming.ArrayFormElement', () => {
  it('renders', () => {
    const fieldName = 'yo'
    const {wrapper} = setup({fieldName})

    expect(wrapper.exists()).toBe(true)
  })
})
