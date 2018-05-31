import React from 'react'
import {shallow} from 'enzyme'
import FuncArg from 'src/ifql/components/FuncArg'
import {service} from 'test/resources'

const setup = () => {
  const props = {
    funcID: '',
    bodyID: '',
    funcName: '',
    declarationID: '',
    argKey: '',
    value: '',
    type: '',
    service,
    onChangeArg: () => {},
    onGenerateScript: () => {},
  }

  const wrapper = shallow(<FuncArg {...props} />)

  return {
    wrapper,
  }
}

describe('IFQL.Components.FuncArg', () => {
  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })
  })
})
