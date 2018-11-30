import React from 'react'
import {shallow} from 'enzyme'

import TelegrafInstructions from 'src/onboarding/components/configureStep/streaming/TelegrafInstructions'

let wrapper

const setup = (override = {}) => {
  const props = {
    authToken: '',
    configID: '',
    ...override,
  }

  return shallow(<TelegrafInstructions {...props} />)
}

describe('TelegrafInstructions', () => {
  it('renders', async () => {
    const wrapper = await setup()
    expect(wrapper.exists()).toBe(true)
  })

  it('matches snapshot', () => {
    wrapper = setup()
    expect(wrapper).toMatchSnapshot()
  })
})
