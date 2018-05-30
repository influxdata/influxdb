import React from 'react'
import {shallow} from 'enzyme'
import From from 'src/ifql/components/From'
import {service} from 'test/resources'

jest.mock('src/ifql/apis', () => require('mocks/ifql/apis'))

const setup = () => {
  const props = {
    funcID: '1',
    argKey: 'db',
    value: 'db1',
    bodyID: '2',
    declarationID: '1',
    service,
    onChangeArg: () => {},
  }

  const wrapper = shallow(<From {...props} />)

  return {
    wrapper,
  }
}

describe('IFQL.Components.From', () => {
  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
