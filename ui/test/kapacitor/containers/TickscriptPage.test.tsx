import React from 'react'
import {shallow} from 'enzyme'
import {TickscriptPage} from 'src/kapacitor/containers/TickscriptPage'
import {source} from 'test/resources'

jest.mock('src/shared/apis', () => require('mocks/shared/apis'))
jest.mock('src/kapacitor/apis', () => require('mocks/kapacitor/apis'))

const setup = () => {
  const props = {
    source,
    errorActions: {
      errorThrown: () => {},
    },
    kapacitorActions: {
      updateTask: () => {},
      createTask: () => {},
      getRule: () => {},
    },
    router: {
      push: () => {},
    },
    params: {
      ruleID: '',
    },
    rules: [],
    notify: () => {},
  }
  const wrapper = shallow(<TickscriptPage {...props} />)

  return {
    wrapper,
  }
}

describe('Kapacitor.Containers.TickscriptPage', () => {
  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
