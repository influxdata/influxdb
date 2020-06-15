// Libraries
import React from 'react'
import {render, fireEvent} from '@testing-library/react'

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

  const wrapper = render(<VariableForm {...props} />)

  return {wrapper, actions}
}

jest.mock('src/shared/components/FluxMonacoEditor', () => () => null)

describe('Variables.Components.VariableForm', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      const {getByTestId} = wrapper
      const root = getByTestId('variable-form--root')

      expect(root).toBeTruthy()
    })
  })

  describe('type selector', () => {
    it('should not tell the the parent if the type was not changed', () => {
      const {wrapper, actions} = setup()
      const {getByTestId} = wrapper

      const button = getByTestId('variable-type-dropdown--button')

      fireEvent.click(button)

      const item = getByTestId('variable-type-dropdown-query')

      fireEvent.click(item)

      expect(actions.type.mock.calls.length).toBe(0)
    })

    it('should tell the parent if type was changed', () => {
      const {wrapper, actions} = setup()
      const {getByTestId} = wrapper

      const button = getByTestId('variable-type-dropdown--button')

      fireEvent.click(button)

      const item = getByTestId('variable-type-dropdown-map')

      fireEvent.click(item)

      expect(actions.type.mock.calls.length).toBe(1)
      expect(actions.type.mock.calls[0][0]).toBe('map')
    })
  })
})
