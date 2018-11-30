// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import DataListening from 'src/onboarding/components/configureStep/streaming/DataListening'
import ConnectionInformation from 'src/onboarding/components/configureStep/streaming/ConnectionInformation'
import {Button} from 'src/clockface'

const setup = (override = {}) => {
  const props = {
    bucket: 'defbuck',
    ...override,
  }

  const wrapper = shallow(<DataListening {...props} />)

  return {wrapper}
}

describe('Onboarding.Components.DataListening', () => {
  it('renders', () => {
    const {wrapper} = setup()
    const button = wrapper.find(Button)

    expect(wrapper.exists()).toBe(true)
    expect(button.exists()).toBe(true)
  })

  describe('if button is clicked', () => {
    it('displays connection information', () => {
      const {wrapper} = setup()

      const button = wrapper.find(Button)
      button.simulate('click')

      const connectionInfo = wrapper.find(ConnectionInformation)

      expect(wrapper.exists()).toBe(true)
      expect(connectionInfo.exists()).toBe(true)
    })
  })
})
