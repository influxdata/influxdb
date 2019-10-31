import React from 'react'
import {shallow} from 'enzyme'

import TelegrafInstructions from 'src/dataLoaders/components/verifyStep/TelegrafInstructions'

const setup = (override = {}) => {
  const props = {
    token: '',
    configID: '',
    ...override,
  }

  return shallow(<TelegrafInstructions {...props} />)
}

describe('TelegrafInstructions', () => {
  it('renders', () => {
    const wrapper = setup()
    expect(wrapper.exists()).toBe(true)
  })
})
