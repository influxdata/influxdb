// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {LineProtocol} from 'src/onboarding/components/configureStep/lineProtocol/LineProtocol'
import {WritePrecision} from 'src/api'

const setup = (override = {}) => {
  const props = {
    bucket: 'a',
    org: 'a',
    onClickNext: jest.fn(),
    onClickBack: jest.fn(),
    onClickSkip: jest.fn(),
    lineProtocolBody: '',
    precision: WritePrecision.Ns,
    setLPStatus: jest.fn(),
    writeLineProtocolAction: jest.fn(),
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
  })
})
