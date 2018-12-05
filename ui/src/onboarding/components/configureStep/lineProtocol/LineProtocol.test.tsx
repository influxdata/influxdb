// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import LineProtocol from 'src/onboarding/components/configureStep/lineProtocol/LineProtocol'

import {LineProtocolStatus} from 'src/types/v2/dataLoaders'

const setup = (override?) => {
  const props = {
    bucket: 'a',
    org: 'a',
    ...override,
  }
  const wrapper = shallow(<LineProtocol {...props} />)

  return {wrapper}
}

describe('LineProtocol', () => {
  describe('rendering', () => {
    it('renders!', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)

      expect(wrapper).toMatchSnapshot()
    })
    it('defaults to selecting importdata tab if no props provided.', () => {
      const {wrapper} = setup()
      expect(wrapper.state('activeCard')).toBe(LineProtocolStatus.ImportData)
    })
  })
})
