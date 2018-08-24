import React from 'react'
import {shallow} from 'enzyme'
import FuncArg from 'src/flux/components/FuncArg'
import {source} from 'test/resources/v2'

const setup = () => {
  const props = {
    funcID: '',
    bodyID: '',
    funcName: '',
    declarationID: '',
    argKey: '',
    args: [],
    value: '',
    type: '',
    source,
    onChangeArg: () => {},
    onGenerateScript: () => {},
  }

  const wrapper = shallow(<FuncArg {...props} />)

  return {
    wrapper,
  }
}

describe('Flux.Components.FuncArg', () => {
  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })
  })
})
