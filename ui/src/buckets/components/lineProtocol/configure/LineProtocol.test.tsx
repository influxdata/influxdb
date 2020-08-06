// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {LineProtocol} from 'src/dataLoaders/components/lineProtocolWizard/configure/LineProtocol'
import {WritePrecision} from '@influxdata/influx'

const setup = (override = {}) => {
  const props = {
    bucket: 'a',
    org: 'a',
    onClickNext: jest.fn(),
    lineProtocolBody: '',
    precision: WritePrecision.Ns,
    setLPStatus: jest.fn(),
    writeLineProtocolAction: jest.fn(),
    currentStepIndex: 0,
    onIncrementCurrentStepIndex: jest.fn(),
    onDecrementCurrentStepIndex: jest.fn(),
    notify: jest.fn(),
    onExit: jest.fn(),
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
    })
  })
})
