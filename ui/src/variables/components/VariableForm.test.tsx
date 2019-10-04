// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import VariableForm from 'src/variables/components/VariableForm'

// Constants
import {variables} from 'mocks/dummyData'

const setup = (override?) => {
  const props = {
    variables,
    initialScript: 'Hello There!',
    onCreateVariable: jest.fn(),
    onHideOverlay: jest.fn(),
    ...override,
  }

  const wrapper = shallow<VariableForm>(<VariableForm {...props} />)

  return {wrapper}
}

describe('VariableForm', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
      expect(wrapper).toMatchSnapshot()
    })
  })

  describe('type selector', () => {
    it('should not update state if the type was not changed', () => {
      const {wrapper} = setup()

      const defaultType = wrapper.state().args.type
      const stateStub = jest.fn()

      wrapper.instance().setState = stateStub

      wrapper.instance()['handleChangeType'](defaultType)
      expect(stateStub.mock.calls.length).toBe(0)
    })

    it('should update state if the type was changed', () => {
      const {wrapper} = setup()

      const stateStub = jest.fn()

      wrapper.instance().setState = stateStub

      wrapper.instance()['handleChangeType']('map')
      expect(stateStub.mock.calls.length).toBe(1)
    })
  })
})
