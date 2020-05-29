// Libraries
import React from 'react'
import {shallow} from 'enzyme'

// Components
import {VariableFormContext} from 'src/variables/components/VariableFormContext'

jest.mock('src/shared/components/FluxMonacoEditor', () => () => null)

const setup = (override?) => {
  const actions = {
    name: jest.fn(),
    type: jest.fn(),
    query: jest.fn(),
    map: jest.fn(),
    constant: jest.fn(),
    clear: jest.fn(),
  }
  const props = {
    initialScript: 'Hello There!',
    onCreateVariable: jest.fn(),
    onHideOverlay: jest.fn(),
    onNameUpdate: name => actions.name(name),
    onTypeUpdate: type => actions.type(type),
    onQueryUpdate: arg => actions.query(arg),
    onMapUpdate: arg => actions.map(arg),
    onConstantUpdate: arg => actions.constant(arg),
    onEditorClose: () => actions.clear(),
    ...override,
  }

  const wrapper = shallow<VariableFormContext>(
    <VariableFormContext {...props} />
  )

  return {wrapper, actions}
}

describe('VariableFormContext', () => {
  it('should tell the store to clear on close', () => {
    const {wrapper, actions} = setup()

    wrapper.instance()['handleHideOverlay']()

    expect(actions.clear.mock.calls.length).toBe(1)
  })
})
