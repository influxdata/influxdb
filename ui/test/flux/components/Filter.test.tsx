import React from 'react'
import {shallow} from 'enzyme'
import {Filter} from 'src/flux/components/Filter'

jest.mock('src/flux/apis', () => require('mocks/flux/apis'))

const setup = (override = {}) => {
  const props = {
    argKey: 'fn',
    funcID: 'f1',
    bodyID: 'b1',
    declarationID: 'd1',
    value: '(r) => r["measurement"] === "m1"',
    onChangeArg: () => {},
    render: () => <div className="test-element" />,
    links: {
      self: '',
      ast: '',
      suggestions: '',
    },
    ...override,
  }

  const wrapper = shallow(<Filter {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Flux.Components.Filter', () => {
  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
