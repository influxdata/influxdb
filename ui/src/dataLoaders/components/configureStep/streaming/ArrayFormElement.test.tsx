// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import ArrayFormElement from 'src/dataLoaders/components/configureStep/streaming/ArrayFormElement'
import {MultipleInput} from 'src/clockface'

import {TelegrafPluginInputCpu} from '@influxdata/influx'

const setup = (override = {}) => {
  const props = {
    fieldName: '',
    addTagValue: jest.fn(),
    removeTagValue: jest.fn(),
    autoFocus: true,
    value: [],
    fieldType: null,
    helpText: '',
    onSetConfigArrayValue: jest.fn(),
    telegrafPluginName: TelegrafPluginInputCpu.NameEnum.Cpu,
    ...override,
  }

  const wrapper = shallow(<ArrayFormElement {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConfigureStep.Streaming.ArrayFormElement', () => {
  it('renders', () => {
    const fieldName = 'yo'
    const {wrapper} = setup({fieldName})
    const multipleInput = wrapper.find(MultipleInput)

    expect(wrapper.exists()).toBe(true)
    expect(multipleInput.exists()).toBe(true)
  })
})
