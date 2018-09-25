import React from 'react'
import {shallow} from 'enzyme'

import ProgressConnector from 'src/clockface/components/wizard/ProgressConnector'
import {ConnectorState} from 'src/clockface/constants/wizard'

describe('Progress Connector', () => {
  let wrapper

  const expectedWithoutStatus = `wizard-progress-connector wizard-progress-connector--${
    ConnectorState.None
  }`

  const expectedWithStatusSome = `wizard-progress-connector wizard-progress-connector--${
    ConnectorState.Some
  }`

  const expectedWithStatusAll = `wizard-progress-connector wizard-progress-connector--${
    ConnectorState.Full
  }`

  describe('without Props', () => {
    const props = {
      status: undefined,
    }

    beforeEach(() => (wrapper = shallow(<ProgressConnector {...props} />)))

    it('mounts without exploding', () => {
      expect(wrapper).toHaveLength(1)
    })

    it('defaults to enum state "none" when no props provided', () => {
      expect(wrapper.find('span').props().className).toBe(expectedWithoutStatus)
    })

    it('matches snapshot when provided minimal props', () => {
      expect(wrapper).toMatchSnapshot()
    })
  })

  describe('with status: some', () => {
    const props = {
      status: ConnectorState.Some,
    }

    beforeEach(() => (wrapper = shallow(<ProgressConnector {...props} />)))

    it('defaults to enum state "some" when no props provided', () => {
      expect(wrapper.find('span').props().className).toBe(
        expectedWithStatusSome
      )
    })

    it('matches snapshot when provided "some" status', () => {
      expect(wrapper).toMatchSnapshot()
    })
  })

  describe('with status: all', () => {
    const props = {
      status: ConnectorState.Full,
    }

    beforeEach(() => (wrapper = shallow(<ProgressConnector {...props} />)))

    it('defaults to enum state "all" when no props provided', () => {
      expect(wrapper.find('span').props().className).toBe(expectedWithStatusAll)
    })

    it('matches snapshot when provided "full" status', () => {
      expect(wrapper).toMatchSnapshot()
    })
  })
})
