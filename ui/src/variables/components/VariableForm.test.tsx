// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import VariableForm from 'src/variables/components/VariableForm'

// Constants
import {variables} from 'mocks/dummyData'

const setup = (override?) => {
  const actions = {
    name: jest.fn(),
    type: jest.fn(),
    query: jest.fn(),
    map: jest.fn(),
    constant: jest.fn(),
  }
  const props = {
    variables,
    variableType: 'query',
    query: {
      type: 'query',
      values: {
        query: '',
        language: 'flux',
      },
    },
    map: {
      type: 'map',
      values: {},
    },
    constant: {
      type: 'constant',
      values: [],
    },
    initialScript: 'Hello There!',
    onCreateVariable: jest.fn(),
    onHideOverlay: jest.fn(),
    onNameUpdate: name => actions.name(name),
    onTypeUpdate: type => actions.type(type),
    onQueryUpdate: arg => actions.query(arg),
    onMapUpdate: arg => actions.map(arg),
    onConstantUpdate: arg => actions.constant(arg),
    ...override,
  }

  const wrapper = shallow<VariableForm>(<VariableForm {...props} />)

  return {wrapper, actions}
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
    it('should not tell the the parrent if the type was not changed', () => {
      const {wrapper, actions} = setup()

      const defaultType = 'query'

      // this way of accessing the instance function
      // bypasses typescript's private function check
      wrapper.instance()['handleChangeType'](defaultType)
      expect(actions.type.mock.calls.length).toBe(0)
    })

    it('should update tell the the parrent if type was changed', () => {
      const {wrapper, actions} = setup()

      // this way of accessing the instance function
      // bypasses typescript's private function check
      wrapper.instance()['handleChangeType']('map')
      expect(actions.type.mock.calls.length).toBe(1)
      expect(actions.type.mock.calls[0][0]).toBe('map')
    })
  })
})
