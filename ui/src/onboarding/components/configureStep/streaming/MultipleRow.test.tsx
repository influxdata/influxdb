// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import MultipleRow from './MultipleRow'

import {TelegrafPluginInputCpu} from 'src/api'

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

  const wrapper = shallow(<MultipleRow {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.ConfigureStep.Streaming.ArrayFormElement', () => {
  it('renders', () => {
    const fieldName = 'yo'
    const {wrapper} = setup({fieldName})

    expect(wrapper.exists()).toBe(true)
  })
})
